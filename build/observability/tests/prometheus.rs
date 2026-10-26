//! Real local collector used by the API and Coordinator acceptance tests.
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

pub struct Collector {
    child: Child,
    address: SocketAddr,
    source: SocketAddr,
    output: PathBuf,
    queries: BTreeMap<String, serde_json::Value>,
}
impl Drop for Collector {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Collector {
    pub async fn start(name: &str, source: SocketAddr) -> Self {
        let executable = std::env::var("ADX_TEST_PROMETHEUS")
            .expect("set ADX_TEST_PROMETHEUS to the native Prometheus executable");
        let output =
            PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").expect("set ADX_TEST_EVIDENCE"))
                .join(name);
        std::fs::create_dir_all(&output).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let config = output.join("prometheus.json");
        std::fs::write(&config, serde_json::to_vec_pretty(&serde_json::json!({
            "global":{"scrape_interval":"1s","scrape_timeout":"900ms"},
            "scrape_configs":[{"job_name":name,"static_configs":[{"targets":[source.to_string()],"labels":{"cluster":"local-observability","adx_env":"local"}}]}]
        })).unwrap()).unwrap();
        let log = std::fs::File::create(output.join("prometheus.log")).unwrap();
        let child = Command::new(executable)
            .arg(format!("--config.file={}", config.display()))
            .arg(format!(
                "--storage.tsdb.path={}",
                output.join("tsdb").display()
            ))
            .arg(format!("--web.listen-address={address}"))
            .arg("--storage.tsdb.retention.time=1h")
            .stdout(log.try_clone().unwrap())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut collector = Self {
            child,
            address,
            source,
            output,
            queries: BTreeMap::new(),
        };
        collector
            .wait_value("up{cluster=\"local-observability\",adx_env=\"local\"}", 1.0)
            .await;
        collector
    }
    pub async fn query(&mut self, query: &str) -> Option<f64> {
        let output = tokio::process::Command::new("curl")
            .args([
                "--silent",
                "--show-error",
                "--fail",
                "--max-time",
                "2",
                "--get",
                "--data-urlencode",
            ])
            .arg(format!("query={query}"))
            .arg(format!("http://{}/api/v1/query", self.address))
            .output()
            .await
            .unwrap();
        if !output.status.success() {
            return None;
        }
        let data: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let value = data["data"]["result"][0]["value"][1]
            .as_str()
            .and_then(|s| s.parse().ok());
        self.queries.insert(query.into(), data);
        value
    }
    pub async fn wait_value(&mut self, query: &str, expected: f64) {
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "collector exited; see {}",
                    self.output.display()
                );
                if self.query(query).await == Some(expected) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap();
    }
    pub async fn capture(&mut self) {
        let output = tokio::process::Command::new("curl")
            .args(["--silent", "--show-error", "--fail", "--max-time", "3"])
            .arg(format!("http://{}/metrics", self.source))
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        std::fs::write(self.output.join("scrape.txt"), output.stdout).unwrap();
        std::fs::write(
            self.output.join("queries.json"),
            serde_json::to_vec_pretty(&self.queries).unwrap(),
        )
        .unwrap();
    }
}
