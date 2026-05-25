use axum::{
    extract::State, http::header::CONTENT_TYPE, response::IntoResponse, routing::get, Router,
};
use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

const LOOP_EXECUTION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug)]
pub struct ConductorMetrics {
    version: String,
    up: AtomicU64,
    websocket_clients: AtomicU64,
    counters: Mutex<Counters>,
}

#[derive(Debug, Default)]
struct Counters {
    healthchecks: BTreeMap<(bool, String), u64>,
    leader_transfers: BTreeMap<bool, u64>,
    sequencer_starts: BTreeMap<bool, u64>,
    sequencer_stops: BTreeMap<bool, u64>,
    state_changes: BTreeMap<(bool, bool, bool), u64>,
    rollup_boost_connection_attempts: BTreeMap<(bool, String), u64>,
    loop_execution_buckets: Vec<u64>,
    loop_execution_count: u64,
    loop_execution_sum: f64,
    rpc_server_requests: BTreeMap<String, u64>,
    rpc_server_responses: BTreeMap<(String, String), u64>,
    rpc_server_params_size: BTreeMap<String, u64>,
    rpc_server_results_size: BTreeMap<String, u64>,
    rpc_server_request_buckets: BTreeMap<String, Vec<u64>>,
    rpc_server_request_count: BTreeMap<String, u64>,
    rpc_server_request_sum: BTreeMap<String, f64>,
    rpc_notifications_received: BTreeMap<String, u64>,
}

impl ConductorMetrics {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            up: AtomicU64::new(0),
            websocket_clients: AtomicU64::new(0),
            counters: Mutex::new(Counters {
                loop_execution_buckets: vec![0; LOOP_EXECUTION_BUCKETS.len()],
                ..Counters::default()
            }),
        }
    }

    pub fn record_up(&self) {
        self.up.store(1, Ordering::Relaxed);
    }

    fn counters(&self) -> MutexGuard<'_, Counters> {
        // Metrics must not become a process-liveness dependency if a recorder panics.
        self.counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn record_health_check(&self, success: bool, error: impl Into<String>) {
        let mut counters = self.counters();
        *counters
            .healthchecks
            .entry((success, error.into()))
            .or_default() += 1;
    }

    pub fn record_leader_transfer(&self, success: bool) {
        let mut counters = self.counters();
        *counters.leader_transfers.entry(success).or_default() += 1;
    }

    pub fn record_start_sequencer(&self, success: bool) {
        let mut counters = self.counters();
        *counters.sequencer_starts.entry(success).or_default() += 1;
    }

    pub fn record_stop_sequencer(&self, success: bool) {
        let mut counters = self.counters();
        *counters.sequencer_stops.entry(success).or_default() += 1;
    }

    pub fn record_state_change(&self, leader: bool, healthy: bool, active: bool) {
        let mut counters = self.counters();
        *counters
            .state_changes
            .entry((leader, healthy, active))
            .or_default() += 1;
    }

    pub fn record_rollup_boost_connection_attempt(&self, success: bool, source: &str) {
        let mut counters = self.counters();
        *counters
            .rollup_boost_connection_attempts
            .entry((success, source.to_string()))
            .or_default() += 1;
    }

    pub fn record_loop_execution_time(&self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        let mut counters = self.counters();
        for (index, bucket) in LOOP_EXECUTION_BUCKETS.iter().enumerate() {
            if seconds <= *bucket {
                counters.loop_execution_buckets[index] += 1;
            }
        }
        counters.loop_execution_count += 1;
        counters.loop_execution_sum += seconds;
    }

    pub fn record_websocket_client_count(&self, count: u64) {
        self.websocket_clients.store(count, Ordering::Relaxed);
    }

    pub fn record_rpc_server_notification(&self, method: &str) {
        let mut counters = self.counters();
        *counters
            .rpc_notifications_received
            .entry(method.to_string())
            .or_default() += 1;
    }

    pub fn record_rpc_server_request(
        &self,
        method: &str,
        params_size: usize,
        duration: Duration,
        error_code: Option<i64>,
        result_size: Option<usize>,
    ) {
        let mut counters = self.counters();
        let method = method.to_string();
        *counters
            .rpc_server_requests
            .entry(method.clone())
            .or_default() += 1;
        *counters
            .rpc_server_params_size
            .entry(method.clone())
            .or_default() += params_size as u64;
        let error = error_code
            .map(|code| format!("rpc_{code}"))
            .unwrap_or_else(|| "<nil>".to_string());
        *counters
            .rpc_server_responses
            .entry((method.clone(), error))
            .or_default() += 1;
        if let Some(result_size) = result_size {
            *counters
                .rpc_server_results_size
                .entry(method.clone())
                .or_default() += result_size as u64;
        }

        let seconds = duration.as_secs_f64();
        let buckets = counters
            .rpc_server_request_buckets
            .entry(method.clone())
            .or_insert_with(|| vec![0; LOOP_EXECUTION_BUCKETS.len()]);
        for (index, bucket) in LOOP_EXECUTION_BUCKETS.iter().enumerate() {
            if seconds <= *bucket {
                buckets[index] += 1;
            }
        }
        *counters
            .rpc_server_request_count
            .entry(method.clone())
            .or_default() += 1;
        *counters.rpc_server_request_sum.entry(method).or_default() += seconds;
    }

    pub fn render_prometheus(&self) -> String {
        let counters = self.counters();
        let mut out = String::new();
        out.push_str("# TYPE op_conductor_info gauge\n");
        push_line(
            &mut out,
            format_args!(
                "op_conductor_info{{version=\"{}\"}} 1",
                escape_label(&self.version)
            ),
        );
        out.push_str("# TYPE op_conductor_up gauge\n");
        push_line(
            &mut out,
            format_args!("op_conductor_up {}", self.up.load(Ordering::Relaxed)),
        );
        out.push_str("# TYPE op_conductor_healthchecks_count counter\n");
        for ((success, error), value) in &counters.healthchecks {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_healthchecks_count{{success=\"{}\",error=\"{}\"}} {}",
                    success,
                    escape_label(error),
                    value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_leader_transfers_count counter\n");
        for (success, value) in &counters.leader_transfers {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_leader_transfers_count{{success=\"{}\"}} {}",
                    success, value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_sequencer_starts_count counter\n");
        for (success, value) in &counters.sequencer_starts {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_sequencer_starts_count{{success=\"{}\"}} {}",
                    success, value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_sequencer_stops_count counter\n");
        for (success, value) in &counters.sequencer_stops {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_sequencer_stops_count{{success=\"{}\"}} {}",
                    success, value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_state_changes_count counter\n");
        for ((leader, healthy, active), value) in &counters.state_changes {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_state_changes_count{{leader=\"{}\",healthy=\"{}\",active=\"{}\"}} {}",
                    leader, healthy, active, value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_rollup_boost_connection_attempts_count counter\n");
        for ((success, source), value) in &counters.rollup_boost_connection_attempts {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rollup_boost_connection_attempts_count{{success=\"{}\",source=\"{}\"}} {}",
                    success,
                    escape_label(source),
                    value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_loop_execution_time histogram\n");
        for (bucket, count) in LOOP_EXECUTION_BUCKETS
            .iter()
            .zip(counters.loop_execution_buckets.iter())
        {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_loop_execution_time_bucket{{le=\"{}\"}} {}",
                    bucket, count
                ),
            );
        }
        push_line(
            &mut out,
            format_args!(
                "op_conductor_loop_execution_time_bucket{{le=\"+Inf\"}} {}",
                counters.loop_execution_count
            ),
        );
        push_line(
            &mut out,
            format_args!(
                "op_conductor_loop_execution_time_sum {}",
                counters.loop_execution_sum
            ),
        );
        push_line(
            &mut out,
            format_args!(
                "op_conductor_loop_execution_time_count {}",
                counters.loop_execution_count
            ),
        );
        out.push_str("# TYPE op_conductor_websocket_clients_connected gauge\n");
        push_line(
            &mut out,
            format_args!(
                "op_conductor_websocket_clients_connected {}",
                self.websocket_clients.load(Ordering::Relaxed)
            ),
        );
        out.push_str("# TYPE op_conductor_rpc_server_requests_total counter\n");
        for (method, value) in &counters.rpc_server_requests {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_requests_total{{rpc=\"main\",method=\"{}\"}} {}",
                    escape_label(method),
                    value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_rpc_server_request_duration_seconds histogram\n");
        for (method, buckets) in &counters.rpc_server_request_buckets {
            for (bucket, count) in LOOP_EXECUTION_BUCKETS.iter().zip(buckets.iter()) {
                push_line(
                    &mut out,
                    format_args!(
                        "op_conductor_rpc_server_request_duration_seconds_bucket{{rpc=\"main\",method=\"{}\",le=\"{}\"}} {}",
                        escape_label(method),
                        bucket,
                        count
                    ),
                );
            }
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_request_duration_seconds_bucket{{rpc=\"main\",method=\"{}\",le=\"+Inf\"}} {}",
                    escape_label(method),
                    counters.rpc_server_request_count.get(method).copied().unwrap_or(0)
                ),
            );
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_request_duration_seconds_sum{{rpc=\"main\",method=\"{}\"}} {}",
                    escape_label(method),
                    counters.rpc_server_request_sum.get(method).copied().unwrap_or(0.0)
                ),
            );
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_request_duration_seconds_count{{rpc=\"main\",method=\"{}\"}} {}",
                    escape_label(method),
                    counters.rpc_server_request_count.get(method).copied().unwrap_or(0)
                ),
            );
        }
        out.push_str("# TYPE op_conductor_rpc_server_responses_total counter\n");
        for ((method, error), value) in &counters.rpc_server_responses {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_responses_total{{rpc=\"main\",method=\"{}\",error=\"{}\"}} {}",
                    escape_label(method),
                    escape_label(error),
                    value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_rpc_server_params_size_total counter\n");
        for (method, value) in &counters.rpc_server_params_size {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_params_size_total{{rpc=\"main\",method=\"{}\"}} {}",
                    escape_label(method),
                    value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_rpc_server_results_size_total counter\n");
        for (method, value) in &counters.rpc_server_results_size {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_server_results_size_total{{rpc=\"main\",method=\"{}\"}} {}",
                    escape_label(method),
                    value
                ),
            );
        }
        out.push_str("# TYPE op_conductor_rpc_client_notifications_received_total counter\n");
        for (method, value) in &counters.rpc_notifications_received {
            push_line(
                &mut out,
                format_args!(
                    "op_conductor_rpc_client_notifications_received_total{{rpc=\"main\",method=\"{}\"}} {}",
                    escape_label(method),
                    value
                ),
            );
        }
        out
    }
}

pub async fn serve_metrics(
    metrics: Arc<ConductorMetrics>,
    addr: SocketAddr,
) -> Result<(), std::io::Error> {
    serve_metrics_with_shutdown(metrics, addr, std::future::pending::<()>()).await
}

pub async fn serve_metrics_with_shutdown<F>(
    metrics: Arc<ConductorMetrics>,
    addr: SocketAddr,
    shutdown: F,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = metrics_router(metrics);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

fn metrics_router(metrics: Arc<ConductorMetrics>) -> Router {
    Router::new()
        .route("/", get(metrics_handler))
        .route("/metrics", get(metrics_handler))
        .fallback(get(metrics_handler))
        .with_state(metrics)
}

async fn metrics_handler(State(metrics): State<Arc<ConductorMetrics>>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics.render_prometheus(),
    )
}

fn push_line(out: &mut String, line: std::fmt::Arguments<'_>) {
    use std::fmt::Write;
    writeln!(out, "{line}").expect("writing to string cannot fail");
}

fn escape_label(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[test]
    fn up_metric_starts_down_until_startup_records_up_like_upstream() {
        let metrics = ConductorMetrics::new("test");

        let rendered = metrics.render_prometheus();

        assert!(rendered.contains("op_conductor_up 0"));

        metrics.record_up();
        let rendered = metrics.render_prometheus();

        assert!(rendered.contains("op_conductor_up 1"));
    }

    #[test]
    fn prometheus_output_uses_upstream_metric_names() {
        let metrics = ConductorMetrics::new("test");
        metrics.record_up();
        metrics.record_health_check(false, "bad \"health\"");
        metrics.record_leader_transfer(true);
        metrics.record_start_sequencer(true);
        metrics.record_stop_sequencer(false);
        metrics.record_state_change(true, false, true);
        metrics.record_rollup_boost_connection_attempt(true, "ws://rollupboost");
        metrics.record_loop_execution_time(Duration::from_millis(25));
        metrics.record_rpc_server_request(
            "conductor_leader",
            2,
            Duration::from_millis(2),
            None,
            Some(4),
        );
        metrics.record_rpc_server_request(
            "conductor_missing",
            2,
            Duration::from_millis(3),
            Some(-32601),
            None,
        );
        metrics.record_rpc_server_notification("conductor_active");

        let rendered = metrics.render_prometheus();

        assert!(rendered.contains("op_conductor_info{version=\"test\"} 1"));
        assert!(rendered.contains(
            "op_conductor_healthchecks_count{success=\"false\",error=\"bad \\\"health\\\"\"} 1"
        ));
        assert!(rendered.contains("op_conductor_leader_transfers_count{success=\"true\"} 1"));
        assert!(rendered.contains("op_conductor_sequencer_starts_count{success=\"true\"} 1"));
        assert!(rendered.contains("op_conductor_sequencer_stops_count{success=\"false\"} 1"));
        assert!(rendered.contains(
            "op_conductor_state_changes_count{leader=\"true\",healthy=\"false\",active=\"true\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rollup_boost_connection_attempts_count{success=\"true\",source=\"ws://rollupboost\"} 1"
        ));
        assert!(rendered.contains("op_conductor_loop_execution_time_bucket{le=\"0.025\"} 1"));
        assert!(rendered.contains("op_conductor_loop_execution_time_count 1"));
        assert!(rendered.contains("op_conductor_websocket_clients_connected 0"));
        assert!(rendered.contains(
            "op_conductor_rpc_server_requests_total{rpc=\"main\",method=\"conductor_leader\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_responses_total{rpc=\"main\",method=\"conductor_leader\",error=\"<nil>\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_responses_total{rpc=\"main\",method=\"conductor_missing\",error=\"rpc_-32601\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_params_size_total{rpc=\"main\",method=\"conductor_leader\"} 2"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_results_size_total{rpc=\"main\",method=\"conductor_leader\"} 4"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_server_request_duration_seconds_count{rpc=\"main\",method=\"conductor_leader\"} 1"
        ));
        assert!(rendered.contains(
            "op_conductor_rpc_client_notifications_received_total{rpc=\"main\",method=\"conductor_active\"} 1"
		));
    }

    #[test]
    fn metrics_continue_after_counter_lock_is_poisoned() {
        let metrics = ConductorMetrics::new("test");

        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = metrics.counters.lock().unwrap();
            panic!("poison metrics counters");
        }));

        assert!(poisoned.is_err());
        let is_poisoned = metrics.counters.lock().is_err();
        assert!(is_poisoned);

        metrics.record_health_check(true, "");
        metrics.record_leader_transfer(true);
        metrics.record_start_sequencer(false);
        metrics.record_stop_sequencer(true);
        metrics.record_state_change(false, true, false);
        metrics.record_rollup_boost_connection_attempt(false, "ws://rollupboost");
        metrics.record_loop_execution_time(Duration::from_millis(1));
        metrics.record_rpc_server_notification("conductor_active");
        metrics.record_rpc_server_request(
            "conductor_leader",
            0,
            Duration::from_micros(500),
            None,
            None,
        );

        let rendered = metrics.render_prometheus();

        assert!(rendered.contains("op_conductor_healthchecks_count{success=\"true\",error=\"\"} 1"));
        assert!(rendered.contains(
            "op_conductor_rpc_server_requests_total{rpc=\"main\",method=\"conductor_leader\"} 1"
        ));
        assert!(rendered.contains("op_conductor_loop_execution_time_count 1"));
    }

    #[tokio::test]
    async fn metrics_server_matches_upstream_handler_mount_paths() {
        let metrics = Arc::new(ConductorMetrics::new("test"));
        metrics.record_up();
        metrics.record_health_check(true, "");
        let app = metrics_router(metrics);

        for path in ["/", "/metrics", "/custom-scrape-path"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK, "path {path}");
            assert_eq!(
                response.headers().get(CONTENT_TYPE).unwrap(),
                "text/plain; version=0.0.4"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let rendered = String::from_utf8(body.to_vec()).unwrap();
            assert!(
                rendered.contains("op_conductor_info{version=\"test\"} 1"),
                "path {path}"
            );
            assert!(
                rendered.contains("op_conductor_healthchecks_count{success=\"true\",error=\"\"} 1"),
                "path {path}"
            );
        }
    }

    #[tokio::test]
    async fn metrics_server_exits_when_shutdown_signal_fires_like_upstream_stop() {
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let addr = "127.0.0.1:0".parse().unwrap();
        let server = tokio::spawn(serve_metrics_with_shutdown(metrics, addr, async move {
            let _ = shutdown_rx.await;
        }));

        shutdown_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();

        assert!(result.is_ok(), "{result:?}");
    }
}
