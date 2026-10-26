//! Optional local Prometheus endpoint; deploy on a private monitoring interface.
use crate::rpc::CoordinatorRpc;
use bytes::Bytes;
use http_body_util::Full;
use std::time::Duration;
use tokio::net::TcpListener;

pub async fn serve(listener: TcpListener, coordinator: CoordinatorRpc) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let coordinator = coordinator.clone();
                    async move {
                        if request.method() == hyper::Method::GET
                            && request.uri().path() == "/observability/environments"
                        {
                            let result = coordinator
                                .interruption_details(request.uri().query().unwrap_or(""))
                                .await;
                            let (status, body) = match result {
                                Ok(body) => (200, body.to_string()),
                                Err(adx_core::Error::Invalid(message)) => {
                                    (400, serde_json::json!({"error":message}).to_string())
                                }
                                Err(_) => (
                                    503,
                                    serde_json::json!({"error":"observation snapshot unavailable"})
                                        .to_string(),
                                ),
                            };
                            let mut response = hyper::Response::new(Full::new(Bytes::from(body)));
                            *response.status_mut() = hyper::StatusCode::from_u16(status)
                                .unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR);
                            response.headers_mut().insert(
                                hyper::header::CONTENT_TYPE,
                                hyper::header::HeaderValue::from_static("application/json"),
                            );
                            return Ok::<_, std::convert::Infallible>(response);
                        }
                        let (status, body) = if request.method() != hyper::Method::GET
                            || !matches!(request.uri().path(), "/metrics" | "/metrics/environments")
                        {
                            (404, String::new())
                        } else {
                            let detail = request.uri().path() == "/metrics/environments";
                            let result = if detail {
                                coordinator.environment_metrics().await
                            } else {
                                coordinator.metrics().await
                            };
                            match result {
                                Ok(mut text) => {
                                    if !detail {
                                        text.push_str(&adx_observability::trace::metrics());
                                        text.push_str(&adx_observability::process::metrics());
                                    }
                                    (200, text)
                                }
                                Err(_) => (503, "metrics temporarily unavailable\n".into()),
                            }
                        };
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .status(status)
                                .header("Content-Type", "text/plain; version=0.0.4")
                                .body(Full::new(Bytes::from(body)))
                                .expect("static metrics response is valid"),
                        )
                    }
                },
            );
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(Duration::from_secs(10), connection).await;
        });
    }
}
