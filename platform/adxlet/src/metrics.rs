//! Sampled backend usage, exposed by the node for the deployment's collector.
use crate::Adxlet;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeUsage {
    pub cpu_usage_ns: u64,
    pub memory_usage_bytes: u64,
    pub memory_limit_bytes: u64,
}
struct Sample {
    runtime: String,
    runtime_class: &'static str,
    usage: RuntimeUsage,
    sampled: Instant,
}
#[derive(Default)]
struct Samples {
    samples: BTreeMap<String, Sample>,
    // Lifetime CPU totals survive removal of an Environment and runtime resets.
    cpu_ns: BTreeMap<&'static str, u128>,
}
#[derive(Default)]
pub(crate) struct Metrics(Mutex<Samples>);
impl Metrics {
    pub fn record(
        &self,
        environment: &str,
        runtime: &str,
        runtime_class: &str,
        usage: RuntimeUsage,
    ) {
        let runtime_class = match runtime_class {
            "runsc" => "runsc",
            "firecracker" => "firecracker",
            "runc" => "runc",
            _ => "other",
        };
        let mut samples = self.0.lock().expect("shared state lock poisoned");
        let previous = samples
            .samples
            .get(environment)
            .filter(|sample| sample.runtime == runtime && sample.runtime_class == runtime_class)
            .map_or(0, |sample| sample.usage.cpu_usage_ns);
        let delta = usage
            .cpu_usage_ns
            .checked_sub(previous)
            .unwrap_or(usage.cpu_usage_ns);
        *samples.cpu_ns.entry(runtime_class).or_default() += u128::from(delta);
        samples.samples.insert(
            environment.into(),
            Sample {
                runtime: runtime.into(),
                runtime_class,
                usage,
                sampled: Instant::now(),
            },
        );
    }
    pub fn remove(&self, environment: &str) {
        self.0
            .lock()
            .expect("shared state lock poisoned")
            .samples
            .remove(environment);
    }
    fn render(&self, detail: bool) -> String {
        let escape = |s: &str| {
            s.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        };
        let samples = self.0.lock().expect("shared state lock poisoned");
        let mut output = String::new();
        if detail {
            for (id, sample) in &samples.samples {
                let labels = format!(
                    "environment_id=\"{}\",runtime_id=\"{}\"",
                    escape(id),
                    escape(&sample.runtime)
                );
                for (name, value) in [
                    (
                        "adx_environment_cpu_usage_seconds_total",
                        sample.usage.cpu_usage_ns as f64 / 1e9,
                    ),
                    (
                        "adx_environment_memory_usage_bytes",
                        sample.usage.memory_usage_bytes as f64,
                    ),
                    (
                        "adx_environment_memory_limit_bytes",
                        sample.usage.memory_limit_bytes as f64,
                    ),
                    (
                        "adx_environment_stats_age_seconds",
                        sample.sampled.elapsed().as_secs_f64(),
                    ),
                ] {
                    output.push_str(&format!("{name}{{{labels}}} {value}\n"));
                }
            }
        } else {
            output.push_str("# TYPE adx_node_environment_cpu_usage_seconds_total counter\n");
            for (class, cpu) in &samples.cpu_ns {
                let mut memory = 0u128;
                let mut limit = 0u128;
                let mut fresh = 0u64;
                let mut stale = 0u64;
                for sample in samples
                    .samples
                    .values()
                    .filter(|sample| sample.runtime_class == *class)
                {
                    if sample.sampled.elapsed().as_secs_f64() > 45.0 {
                        stale += 1;
                    } else {
                        fresh += 1;
                        memory += u128::from(sample.usage.memory_usage_bytes);
                        limit += u128::from(sample.usage.memory_limit_bytes);
                    }
                }
                output.push_str(&format!("adx_node_environment_cpu_usage_seconds_total{{runtime_class=\"{class}\"}} {}\n", *cpu as f64 / 1e9));
                for (name, value) in [
                    ("memory_usage_bytes", memory),
                    ("memory_limit_bytes", limit),
                    ("samples", u128::from(fresh)),
                    ("stale_samples", u128::from(stale)),
                ] {
                    output.push_str(&format!(
                        "adx_node_environment_{name}{{runtime_class=\"{class}\"}} {value}\n"
                    ));
                }
            }
        }
        output
    }
}
impl Adxlet {
    /// Opt-in short-retention resource samples, separate from the node summary.
    pub fn environment_metrics(&self) -> String {
        self.services.metrics.render(true)
    }
    pub fn metrics(&self) -> String {
        self.prune_retired_environments();
        let mut output = self.services.metrics.render(false);
        output.push_str(&adx_observability::trace::metrics());
        output.push_str(&adx_observability::process::metrics());
        let managed_environments = u64::try_from(
            self.environments
                .lock()
                .expect("shared state lock poisoned")
                .len(),
        )
        .unwrap_or(u64::MAX);
        let retired_environments = u64::try_from(
            self.retired_environments
                .lock()
                .expect("shared state lock poisoned")
                .len(),
        )
        .unwrap_or(u64::MAX);
        let lifecycle_ready =
            self.lifecycle_ready.try_read().is_ok_and(|ready| *ready) && !self.is_draining();
        let admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        let fresh = admission
            .valid_until
            .is_some_and(|until| tokio::time::Instant::now() < until);
        let devices_fresh = admission
            .devices_valid_until
            .is_some_and(|until| tokio::time::Instant::now() < until);
        let accepting = lifecycle_ready && fresh && !admission.maintenance && !admission.pressure;
        let mut text = adx_observability::metrics::Text::default();
        text.gauge("adx_node_accepting_allocations", &[], u64::from(accepting));
        text.gauge("adx_node_resource_observation_fresh", &[], u64::from(fresh));
        text.gauge(
            "adx_node_device_observation_fresh",
            &[],
            u64::from(devices_fresh),
        );
        text.gauge("adx_node_managed_environments", &[], managed_environments);
        text.gauge(
            "adx_node_retired_environment_tombstones",
            &[],
            retired_environments,
        );
        adx_observability::metrics::resources(
            &mut text,
            "adx_node",
            &[],
            &admission.ledger,
            &admission.devices,
            accepting,
            devices_fresh,
        );
        output.push_str(&text.finish());
        output
    }
}
pub async fn serve(manager: Arc<Adxlet>, bind: std::net::SocketAddr) -> std::io::Result<()> {
    use bytes::Bytes;
    use http_body_util::Full;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let manager = manager.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let valid = request.method() == hyper::Method::GET
                        && matches!(request.uri().path(), "/metrics" | "/metrics/environments");
                    let detail = request.uri().path() == "/metrics/environments";
                    let response = hyper::Response::builder()
                        .status(if valid { 200 } else { 404 })
                        .header("Content-Type", "text/plain; version=0.0.4")
                        .body(Full::new(Bytes::from(if valid {
                            if detail {
                                manager.environment_metrics()
                            } else {
                                manager.metrics()
                            }
                        } else {
                            String::new()
                        })))
                        .expect("static metrics response is valid");
                    async move { Ok::<_, std::convert::Infallible>(response) }
                },
            );
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), connection).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(cpu: u64, memory: u64) -> RuntimeUsage {
        RuntimeUsage {
            cpu_usage_ns: cpu,
            memory_usage_bytes: memory,
            memory_limit_bytes: 1024,
        }
    }

    #[test]
    fn aggregate_cpu_survives_runtime_removal_and_counter_resets() {
        let metrics = Metrics::default();
        metrics.record("a", "a-1", "runsc", usage(1_000_000_000, 100));
        metrics.record("b", "b-1", "firecracker", usage(2_000_000_000, 200));
        metrics.record("a", "a-1", "runsc", usage(3_000_000_000, 110));
        metrics.remove("a");
        metrics.record("a", "a-2", "runsc", usage(500_000_000, 120));
        metrics.record("a", "a-2", "runsc", usage(250_000_000, 130));
        let aggregate = metrics.render(false);
        assert!(
            aggregate.contains(
                "adx_node_environment_cpu_usage_seconds_total{runtime_class=\"runsc\"} 3.75\n"
            ),
            "{aggregate}"
        );
        assert!(aggregate
            .contains("adx_node_environment_memory_usage_bytes{runtime_class=\"runsc\"} 130\n"));
        assert!(!aggregate.contains("environment_id="));
        let detail = metrics.render(true);
        assert!(detail.contains("environment_id=\"a\",runtime_id=\"a-2\""));
        assert!(!detail.contains("a-1"));
        assert!(!detail.contains("adx_node_environment_"));
    }

    #[test]
    fn stale_samples_are_not_current_resource_usage() {
        let metrics = Metrics::default();
        metrics.record("a", "a-1", "runsc", usage(10, 100));
        metrics
            .0
            .lock()
            .unwrap()
            .samples
            .get_mut("a")
            .unwrap()
            .sampled = Instant::now() - std::time::Duration::from_secs(46);
        let aggregate = metrics.render(false);
        assert!(aggregate.contains("adx_node_environment_samples{runtime_class=\"runsc\"} 0\n"));
        assert!(
            aggregate.contains("adx_node_environment_stale_samples{runtime_class=\"runsc\"} 1\n")
        );
        assert!(aggregate
            .contains("adx_node_environment_memory_usage_bytes{runtime_class=\"runsc\"} 0\n"));
        assert!(metrics
            .render(true)
            .contains("adx_environment_stats_age_seconds"));
    }
}
