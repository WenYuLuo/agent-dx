use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn active_and_completed_bytes_are_monotonic() {
    let meter = Arc::new(TrafficMeter::default());
    let (client, mut peer) = tokio::io::duplex(32);
    let mut client = MeteredIo::new(client, &meter);
    client.write_all(b"request").await.unwrap();
    let mut bytes = [0; 7];
    peer.read_exact(&mut bytes).await.unwrap();
    peer.write_all(b"reply").await.unwrap();
    client.read_exact(&mut bytes[..5]).await.unwrap();
    assert_eq!(meter.snapshot(), (5, 7));
    drop(client);
    assert_eq!(meter.snapshot(), (5, 7));
    let counters = ByteCounters::default();
    let guard = meter.register(counters.clone());
    counters.up.store(11, Ordering::Relaxed);
    counters.down.store(13, Ordering::Relaxed);
    assert_eq!(meter.snapshot(), (16, 20));
    drop(guard);
    assert_eq!(meter.snapshot(), (16, 20));
}

#[test]
fn scrape_racing_connection_close_never_double_counts_or_loses_bytes() {
    let meter = Arc::new(TrafficMeter::default());
    let writer = meter.clone();
    let task = std::thread::spawn(move || {
        for _ in 0..1000 {
            let counters = ByteCounters::default();
            let guard = writer.register(counters.clone());
            counters.up.store(17, Ordering::Relaxed);
            counters.down.store(19, Ordering::Relaxed);
            drop(guard);
        }
    });
    let mut last = (0, 0);
    while !task.is_finished() {
        let now = meter.snapshot();
        assert!(now.0 >= last.0 && now.1 >= last.1);
        assert!(now.0 <= 17_000 && now.1 <= 19_000);
        last = now;
    }
    task.join().unwrap();
    assert_eq!(meter.snapshot(), (17_000, 19_000));
}

// Manual A/B probe: same loopback proxy and payload for both modes. The plain
// mode is test-only; production always reports traffic. Run in release profile
// with ADX_TRAFFIC_BENCH_MODE=plain|metered and /usr/bin/time for process CPU.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual release-profile throughput/latency comparison"]
async fn loopback_traffic_overhead() {
    use std::time::Instant;
    use tokio::net::{TcpListener, TcpStream};
    let metered = std::env::var("ADX_TRAFFIC_BENCH_MODE").unwrap() == "metered";
    let meter = Arc::new(TrafficMeter::default());
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend.local_addr().unwrap();
    let echo = tokio::spawn(async move {
        loop {
            let (stream, _) = backend.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            tokio::spawn(async move {
                let (mut read, mut write) = stream.into_split();
                tokio::io::copy(&mut read, &mut write).await.unwrap();
            });
        }
    });
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let registry = meter.clone();
    let forward = tokio::spawn(async move {
        loop {
            let (stream, _) = proxy.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            let registry = registry.clone();
            tokio::spawn(async move {
                let mut upstream = TcpStream::connect(backend_addr).await.unwrap();
                upstream.set_nodelay(true).unwrap();
                if metered {
                    let mut stream = MeteredIo::new(stream, &registry);
                    tokio::io::copy_bidirectional(&mut stream, &mut upstream)
                        .await
                        .unwrap();
                } else {
                    let mut stream = stream;
                    tokio::io::copy_bidirectional(&mut stream, &mut upstream)
                        .await
                        .unwrap();
                }
            });
        }
    });
    // Keep realistic scrape activity concurrent with forwarding.
    let scrape = tokio::spawn(async move {
        loop {
            let _ = meter.snapshot();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        tasks.push(tokio::spawn(async move {
            let mut client = TcpStream::connect(proxy_addr).await.unwrap();
            client.set_nodelay(true).unwrap();
            let body = vec![0x5a; 64 * 1024];
            let mut reply = vec![0; body.len()];
            for _ in 0..512 {
                client.write_all(&body).await.unwrap();
                client.read_exact(&mut reply).await.unwrap();
                assert_eq!(reply, body);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let throughput = 512.0 / started.elapsed().as_secs_f64(); // both directions, MiB/s
    let mut times = Vec::new();
    let mut client = TcpStream::connect(proxy_addr).await.unwrap();
    client.set_nodelay(true).unwrap();
    for _ in 0..2000 {
        let now = Instant::now();
        client.write_all(&[0x5a; 1024]).await.unwrap();
        client.read_exact(&mut [0; 1024]).await.unwrap();
        times.push(now.elapsed().as_nanos());
    }
    times.sort_unstable();
    println!(
        "TRAFFIC_BENCH mode={} throughput_mib_s={:.2} p50_us={:.2} p95_us={:.2} p99_us={:.2}",
        if metered { "metered" } else { "plain" },
        throughput,
        times[1000] as f64 / 1000.0,
        times[1900] as f64 / 1000.0,
        times[1980] as f64 / 1000.0
    );
    forward.abort();
    echo.abort();
    scrape.abort();
}
