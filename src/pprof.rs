use anyhow::Context;
use axum::{
    extract::Query,
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use pprof::protos::Message;
use serde::Deserialize;
use std::{
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf, MAIN_SEPARATOR},
    time::Duration,
};

const TEXT: &str = "text/plain; charset=utf-8";
const PROFILE_PROTO: &str = "application/octet-stream";
const DEFAULT_PROFILE_SECONDS: u64 = 30;
const MAX_PROFILE_SECONDS: u64 = 300;
const DEFAULT_SAMPLE_FREQUENCY: i32 = 100;
const CPU_PROFILE_FILENAME: &str = "cpu.prof";

pub struct CpuFileProfiler {
    target: PathBuf,
    guard: Option<pprof::ProfilerGuard<'static>>,
}

impl CpuFileProfiler {
    pub fn target(&self) -> &Path {
        &self.target
    }

    pub fn finish(mut self) -> anyhow::Result<PathBuf> {
        let guard = self
            .guard
            .take()
            .context("CPU file profiler already finished")?;
        let body = build_profile_body(&guard)?;
        std::fs::write(&self.target, body)
            .with_context(|| format!("failed to write CPU profile {}", self.target.display()))?;
        Ok(self.target)
    }
}

pub fn start_cpu_file_profiler(path: Option<&Path>) -> anyhow::Result<CpuFileProfiler> {
    let target = profile_target_path(path, CPU_PROFILE_FILENAME);
    let guard = start_profiler()?;
    Ok(CpuFileProfiler {
        target,
        guard: Some(guard),
    })
}

pub async fn serve_pprof(addr: SocketAddr) -> Result<(), std::io::Error> {
    serve_pprof_with_shutdown(addr, std::future::pending::<()>()).await
}

pub async fn serve_pprof_with_shutdown<F>(
    addr: SocketAddr,
    shutdown: F,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, pprof_router())
        .with_graceful_shutdown(shutdown)
        .await
}

pub fn pprof_router() -> Router {
    Router::new()
        .route("/debug/pprof", get(pprof_index))
        .route("/debug/pprof/", get(pprof_index))
        .route("/debug/pprof/profile", get(cpu_profile))
        .route("/debug/pprof/symbol", get(symbol).post(symbol))
        .route("/debug/pprof/trace", get(trace))
        .route("/debug/pprof/allocs", get(runtime_profile))
        .route("/debug/pprof/block", get(runtime_profile))
        .route("/debug/pprof/cmdline", get(cmdline))
        .route("/debug/pprof/goroutine", get(runtime_profile))
        .route("/debug/pprof/heap", get(runtime_profile))
        .route("/debug/pprof/mutex", get(runtime_profile))
        .route("/debug/pprof/threadcreate", get(runtime_profile))
}

async fn pprof_index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
<head><title>/debug/pprof/</title></head>
<body>
<h1>/debug/pprof/</h1>
<ul>
<li><a href="/debug/pprof/profile">profile</a></li>
<li><a href="/debug/pprof/trace">trace</a></li>
<li><a href="/debug/pprof/symbol">symbol</a></li>
<li><a href="/debug/pprof/allocs">allocs</a></li>
<li><a href="/debug/pprof/block">block</a></li>
<li><a href="/debug/pprof/goroutine">goroutine</a></li>
<li><a href="/debug/pprof/heap">heap</a></li>
<li><a href="/debug/pprof/mutex">mutex</a></li>
<li><a href="/debug/pprof/threadcreate">threadcreate</a></li>
</ul>
</body>
</html>"#,
    )
}

#[derive(Debug, Deserialize)]
struct ProfileQuery {
    seconds: Option<u64>,
}

async fn cpu_profile(Query(query): Query<ProfileQuery>) -> Response {
    let result = tokio::task::spawn_blocking(move || collect_cpu_profile(query)).await;
    match result {
        Ok(Ok(body)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, PROFILE_PROTO)],
            body,
        )
            .into_response(),
        Ok(Err(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, TEXT)],
            err,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, TEXT)],
            format!("profile collection task failed: {err}\n"),
        )
            .into_response(),
    }
}

async fn trace() -> Response {
    unsupported("trace collection is not available in conductor-rs").into_response()
}

async fn runtime_profile() -> Response {
    unsupported("this Go runtime pprof profile is not available in conductor-rs").into_response()
}

async fn symbol() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, TEXT)],
        "num_symbols: 0\n",
    )
        .into_response()
}

async fn cmdline() -> Response {
    let mut args = std::env::args().collect::<Vec<_>>().join("\0");
    args.push('\n');
    (StatusCode::OK, [(header::CONTENT_TYPE, TEXT)], args).into_response()
}

fn unsupported(
    message: &'static str,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
    (
        StatusCode::NOT_IMPLEMENTED,
        [(header::CONTENT_TYPE, TEXT)],
        format!("{message}\n"),
    )
}

fn collect_cpu_profile(query: ProfileQuery) -> Result<Vec<u8>, String> {
    let seconds = query
        .seconds
        .unwrap_or(DEFAULT_PROFILE_SECONDS)
        .min(MAX_PROFILE_SECONDS);
    let guard = start_profiler().map_err(|err| format!("failed to start CPU profiler: {err}\n"))?;

    std::thread::sleep(Duration::from_secs(seconds));

    build_profile_body(&guard).map_err(|err| format!("{err:#}\n"))
}

fn start_profiler() -> anyhow::Result<pprof::ProfilerGuard<'static>> {
    pprof::ProfilerGuardBuilder::default()
        .frequency(DEFAULT_SAMPLE_FREQUENCY)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .context("failed to start CPU profiler")
}

fn build_profile_body(guard: &pprof::ProfilerGuard<'_>) -> anyhow::Result<Vec<u8>> {
    let report = guard
        .report()
        .build()
        .context("failed to build CPU profile report")?;
    let profile = report
        .pprof()
        .context("failed to encode CPU profile report")?;
    let mut body = Vec::new();
    profile
        .encode(&mut body)
        .context("failed to serialize CPU profile report")?;
    Ok(body)
}

fn profile_target_path(path: Option<&Path>, default_filename: &str) -> PathBuf {
    match path {
        Some(path) if is_profile_dir_path(path) => path.join(default_filename),
        Some(path) => path.to_path_buf(),
        None => PathBuf::from(default_filename),
    }
}

fn is_profile_dir_path(path: &Path) -> bool {
    if path.metadata().is_ok_and(|metadata| metadata.is_dir()) {
        return true;
    }

    // Match op-service's PathFlag behavior for a non-existing path that still
    // carries directory intent, e.g. `--pprof.path /tmp/profiles/`.
    path.to_string_lossy().ends_with(MAIN_SEPARATOR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    static TEST_PROFILE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn pprof_index_exposes_upstream_route_surface() {
        let response = pprof_router()
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("/debug/pprof/profile"));
        assert!(body.contains("/debug/pprof/heap"));
        assert!(body.contains("/debug/pprof/goroutine"));
    }

    #[tokio::test]
    async fn pprof_cpu_profile_returns_pprof_proto() {
        let _guard = TEST_PROFILE_LOCK.lock().await;
        let response = pprof_router()
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/profile?seconds=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            PROFILE_PROTO
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!body.is_empty());
    }

    #[tokio::test]
    async fn cpu_file_profile_writes_profile_to_target_path() {
        let _guard = TEST_PROFILE_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();

        let profiler = start_cpu_file_profiler(Some(dir.path())).unwrap();
        assert_eq!(profiler.target(), dir.path().join(CPU_PROFILE_FILENAME));
        let target = profiler.finish().unwrap();

        assert!(target.exists());
        assert!(std::fs::metadata(target).unwrap().len() > 0);
    }

    #[test]
    fn profile_target_path_preserves_explicit_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("profile.out");

        assert_eq!(
            profile_target_path(Some(&target), CPU_PROFILE_FILENAME),
            target
        );
    }

    #[test]
    fn profile_target_path_uses_default_file_for_directory_style_path() {
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("profiles");
        let directory_style_path =
            PathBuf::from(format!("{}{}", target_dir.display(), MAIN_SEPARATOR));

        assert_eq!(
            profile_target_path(Some(&directory_style_path), CPU_PROFILE_FILENAME),
            target_dir.join(CPU_PROFILE_FILENAME)
        );
    }

    #[tokio::test]
    async fn pprof_go_runtime_profiles_are_explicitly_unsupported() {
        let response = pprof_router()
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/goroutine")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(std::str::from_utf8(&body).unwrap().contains("Go runtime"));
    }

    #[tokio::test]
    async fn pprof_server_exits_when_shutdown_signal_fires_like_upstream_stop() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let addr = "127.0.0.1:0".parse().unwrap();
        let server = tokio::spawn(serve_pprof_with_shutdown(addr, async move {
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
