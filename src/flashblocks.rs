use crate::metrics::ConductorMetrics;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite};
use url::Url;

type UpstreamWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const LEADER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const UPSTREAM_DIAL_TIMEOUT: Duration = Duration::from_secs(5);
const UPSTREAM_READ_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_CONNECTION_ATTEMPTS: usize = 5;
const DOWNSTREAM_PING_INTERVAL: Duration = Duration::from_secs(15);
const DOWNSTREAM_PONG_TIMEOUT: Duration = Duration::from_secs(10);
const DOWNSTREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const SEND_CHANNEL_BUFFER_SIZE: usize = 256;

#[derive(Clone, Debug)]
pub struct FlashblocksConfig {
    pub listen_addr: SocketAddr,
    pub rollup_boost_ws_url: Url,
}

#[derive(Debug, Error)]
pub enum FlashblocksError {
    #[error("flashblocks websocket URL must use ws or wss, got {0}")]
    InvalidScheme(String),
    #[error("failed to connect to rollup boost WebSocket at {url}: {error}")]
    InitialConnection { url: String, error: String },
    #[error("websocket server error: {0}")]
    Io(#[from] std::io::Error),
    #[error("websocket server task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug)]
pub struct FlashblocksRuntime {
    local_addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    server_task: JoinHandle<Result<(), std::io::Error>>,
    upstream_task: JoinHandle<()>,
}

impl FlashblocksRuntime {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn wait(self) -> Result<(), FlashblocksError> {
        self.server_task.await??;
        self.upstream_task.abort();
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), FlashblocksError> {
        let _ = self.shutdown.send(true);
        self.server_task.await??;
        self.upstream_task.await?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Hub {
    inner: Arc<HubInner>,
}

#[derive(Debug)]
struct HubInner {
    clients: Mutex<BTreeMap<u64, mpsc::Sender<WsPayload>>>,
    next_id: AtomicU64,
    metrics: Arc<ConductorMetrics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WsPayload(Vec<u8>);

impl WsPayload {
    fn into_downstream_message(self) -> Message {
        // Go op-conductor broadcasts rollup-boost bytes as downstream text frames.
        Message::Text(String::from_utf8_lossy(&self.0).into_owned())
    }
}

pub async fn start_flashblocks(
    config: FlashblocksConfig,
    metrics: Arc<ConductorMetrics>,
    is_leader: Arc<dyn Fn() -> bool + Send + Sync>,
) -> Result<FlashblocksRuntime, FlashblocksError> {
    validate_ws_url(&config.rollup_boost_ws_url)?;
    start_flashblocks_with_initial_attempts(
        config,
        metrics,
        is_leader,
        INITIAL_CONNECTION_ATTEMPTS,
        RECONNECT_DELAY,
    )
    .await
}

async fn start_flashblocks_with_initial_attempts(
    config: FlashblocksConfig,
    metrics: Arc<ConductorMetrics>,
    is_leader: Arc<dyn Fn() -> bool + Send + Sync>,
    initial_attempts: usize,
    reconnect_delay: Duration,
) -> Result<FlashblocksRuntime, FlashblocksError> {
    let initial_upstream = connect_initial_upstream(
        &config.rollup_boost_ws_url,
        initial_attempts,
        reconnect_delay,
    )
    .await?;
    let listener = TcpListener::bind(config.listen_addr).await?;
    let local_addr = listener.local_addr()?;
    let hub = Hub::new(metrics.clone());
    let app = Router::new()
        .route("/ws", get(handle_ws))
        .with_state(hub.clone());
    let (shutdown, shutdown_rx) = watch::channel(false);
    let server_shutdown = shutdown_rx.clone();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(server_shutdown))
            .await
    });
    let upstream_task = tokio::spawn(run_upstream_listener(
        config.rollup_boost_ws_url,
        Some(initial_upstream),
        hub,
        metrics,
        is_leader,
        reconnect_delay,
        shutdown_rx,
    ));

    Ok(FlashblocksRuntime {
        local_addr,
        shutdown,
        server_task,
        upstream_task,
    })
}

pub async fn serve_flashblocks(
    config: FlashblocksConfig,
    metrics: Arc<ConductorMetrics>,
    is_leader: Arc<dyn Fn() -> bool + Send + Sync>,
) -> Result<(), FlashblocksError> {
    start_flashblocks(config, metrics, is_leader)
        .await?
        .wait()
        .await
}

async fn handle_ws(ws: WebSocketUpgrade, State(hub): State<Hub>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_downstream(socket, hub))
}

async fn handle_downstream(socket: WebSocket, hub: Hub) {
    handle_downstream_with_timing(
        socket,
        hub,
        DOWNSTREAM_PING_INTERVAL,
        DOWNSTREAM_PONG_TIMEOUT,
        DOWNSTREAM_WRITE_TIMEOUT,
    )
    .await;
}

async fn handle_downstream_with_timing(
    socket: WebSocket,
    hub: Hub,
    ping_interval: Duration,
    pong_timeout: Duration,
    write_timeout: Duration,
) {
    let (client_id, mut rx) = hub.register().await;
    let (mut sender, mut receiver) = socket.split();
    let mut ping = tokio::time::interval(ping_interval);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping.tick().await;
    let mut awaiting_pong = false;
    let mut pong_deadline = Box::pin(tokio::time::sleep(Duration::from_secs(365 * 24 * 60 * 60)));

    loop {
        tokio::select! {
            Some(payload) = rx.recv() => {
                let message = payload.into_downstream_message();
                let write_result = tokio::time::timeout(
                    write_timeout,
                    sender.send(message)
                ).await;
                if !matches!(write_result, Ok(Ok(()))) {
                    break;
                }
            }
            _ = ping.tick() => {
                let ping_result = tokio::time::timeout(
                    write_timeout,
                    sender.send(Message::Ping(Vec::new()))
                ).await;
                if !matches!(ping_result, Ok(Ok(()))) {
                    break;
                }
                awaiting_pong = true;
                pong_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + pong_timeout);
            }
            _ = &mut pong_deadline, if awaiting_pong => {
                break;
            }
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Pong(_))) => awaiting_pong = false,
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }

    hub.unregister(client_id).await;
}

async fn run_upstream_listener(
    rollup_boost_ws_url: Url,
    initial_upstream: Option<UpstreamWebSocket>,
    hub: Hub,
    metrics: Arc<ConductorMetrics>,
    is_leader: Arc<dyn Fn() -> bool + Send + Sync>,
    reconnect_delay: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let source = rollup_boost_ws_url.to_string();
    let mut upstream = initial_upstream;
    loop {
        if *shutdown.borrow() {
            break;
        }

        if upstream.is_none() {
            match dial_upstream(&rollup_boost_ws_url).await {
                Ok(stream) => {
                    metrics.record_rollup_boost_connection_attempt(true, &source);
                    upstream = Some(stream);
                }
                Err(_) => {
                    metrics.record_rollup_boost_connection_attempt(false, &source);
                    sleep_or_shutdown(reconnect_delay, &mut shutdown).await;
                    continue;
                }
            }
        }

        if !is_leader() {
            sleep_or_shutdown(LEADER_POLL_INTERVAL, &mut shutdown).await;
            continue;
        }

        let Some(stream) = upstream.as_mut() else {
            continue;
        };
        let read = tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
            read = tokio::time::timeout(UPSTREAM_READ_TIMEOUT, stream.next()) => read,
        };
        match read {
            Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                if is_leader() {
                    hub.broadcast(WsPayload(text.to_string().into_bytes()))
                        .await;
                }
            }
            Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
                if is_leader() {
                    hub.broadcast(WsPayload(bytes.to_vec())).await;
                }
            }
            Ok(Some(Ok(tungstenite::Message::Close(_)))) | Ok(None) => {
                upstream = None;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Err(_) => {
                upstream = None;
            }
        }
    }
}

async fn connect_initial_upstream(
    rollup_boost_ws_url: &Url,
    initial_attempts: usize,
    reconnect_delay: Duration,
) -> Result<UpstreamWebSocket, FlashblocksError> {
    let attempts = initial_attempts.max(1);
    let mut last_error = "no connection attempts made".to_string();
    for attempt in 1..=attempts {
        match dial_upstream(rollup_boost_ws_url).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                last_error = err;
                if attempt < attempts {
                    tokio::time::sleep(reconnect_delay).await;
                }
            }
        }
    }
    Err(FlashblocksError::InitialConnection {
        url: rollup_boost_ws_url.to_string(),
        error: last_error,
    })
}

async fn dial_upstream(rollup_boost_ws_url: &Url) -> Result<UpstreamWebSocket, String> {
    match tokio::time::timeout(
        UPSTREAM_DIAL_TIMEOUT,
        connect_async(rollup_boost_ws_url.as_str()),
    )
    .await
    {
        Ok(Ok((stream, _))) => Ok(stream),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err(format!("timed out after {UPSTREAM_DIAL_TIMEOUT:?}")),
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = shutdown.changed() => {}
    }
}

fn validate_ws_url(url: &Url) -> Result<(), FlashblocksError> {
    match url.scheme() {
        "ws" | "wss" => Ok(()),
        other => Err(FlashblocksError::InvalidScheme(other.to_string())),
    }
}

impl Hub {
    fn new(metrics: Arc<ConductorMetrics>) -> Self {
        Self {
            inner: Arc::new(HubInner {
                clients: Mutex::new(BTreeMap::new()),
                next_id: AtomicU64::new(1),
                metrics,
            }),
        }
    }

    async fn register(&self) -> (u64, mpsc::Receiver<WsPayload>) {
        let client_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(SEND_CHANNEL_BUFFER_SIZE);
        let mut clients = self.inner.clients.lock().await;
        clients.insert(client_id, tx);
        self.inner
            .metrics
            .record_websocket_client_count(clients.len() as u64);
        (client_id, rx)
    }

    async fn unregister(&self, client_id: u64) {
        let mut clients = self.inner.clients.lock().await;
        clients.remove(&client_id);
        self.inner
            .metrics
            .record_websocket_client_count(clients.len() as u64);
    }

    async fn broadcast(&self, payload: WsPayload) {
        let mut clients = self.inner.clients.lock().await;
        let mut dropped = Vec::new();
        for (client_id, tx) in clients.iter() {
            if tx.try_send(payload.clone()).is_err() {
                dropped.push(*client_id);
            }
        }
        for client_id in dropped {
            clients.remove(&client_id);
        }
        self.inner
            .metrics
            .record_websocket_client_count(clients.len() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::ws::WebSocketUpgrade;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn forwards_rollup_boost_messages_when_leader() {
        let upstream = TestUpstream::start(vec!["hello", "world"]).await;
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let runtime = start_flashblocks(
            FlashblocksConfig {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                rollup_boost_ws_url: upstream.url.clone(),
            },
            metrics,
            Arc::new(|| true),
        )
        .await
        .unwrap();
        upstream.wait_connected().await;

        let downstream_url = format!("ws://{}/ws", runtime.local_addr());
        let (mut downstream, _) = connect_async(&downstream_url).await.unwrap();
        upstream.release_messages();

        assert_eq!(next_text(&mut downstream).await, "hello");
        assert_eq!(next_text(&mut downstream).await, "world");
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn forwards_binary_rollup_boost_messages_as_text_frames() {
        let upstream = TestUpstream::start_frames(vec![TestFrame::Binary(
            br#"{"flashblock":"binary"}"#.to_vec(),
        )])
        .await;
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let runtime = start_flashblocks(
            FlashblocksConfig {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                rollup_boost_ws_url: upstream.url.clone(),
            },
            metrics,
            Arc::new(|| true),
        )
        .await
        .unwrap();
        upstream.wait_connected().await;

        let downstream_url = format!("ws://{}/ws", runtime.local_addr());
        let (mut downstream, _) = connect_async(&downstream_url).await.unwrap();
        upstream.release_messages();

        let message = tokio::time::timeout(Duration::from_secs(2), downstream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match message {
            tungstenite::Message::Text(text) => {
                assert_eq!(text.as_str(), r#"{"flashblock":"binary"}"#);
            }
            other => panic!("expected downstream text websocket message, got {other:?}"),
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn does_not_forward_rollup_boost_messages_when_follower() {
        let upstream = TestUpstream::start(vec!["leader-only"]).await;
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let runtime = start_flashblocks(
            FlashblocksConfig {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                rollup_boost_ws_url: upstream.url.clone(),
            },
            metrics,
            Arc::new(|| false),
        )
        .await
        .unwrap();
        upstream.wait_connected().await;

        let downstream_url = format!("ws://{}/ws", runtime.local_addr());
        let (mut downstream, _) = connect_async(&downstream_url).await.unwrap();
        upstream.release_messages();

        let no_message = tokio::time::timeout(Duration::from_millis(250), downstream.next()).await;
        assert!(no_message.is_err());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejects_non_websocket_rollup_boost_url() {
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let err = start_flashblocks(
            FlashblocksConfig {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                rollup_boost_ws_url: "http://127.0.0.1/ws".parse().unwrap(),
            },
            metrics,
            Arc::new(|| true),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, FlashblocksError::InvalidScheme(_)));
    }

    #[tokio::test]
    async fn fails_startup_when_initial_rollup_boost_connection_fails() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unused_addr = listener.local_addr().unwrap();
        drop(listener);
        let metrics = Arc::new(ConductorMetrics::new("test"));

        let err = start_flashblocks_with_initial_attempts(
            FlashblocksConfig {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                rollup_boost_ws_url: format!("ws://{unused_addr}/ws").parse().unwrap(),
            },
            metrics,
            Arc::new(|| true),
            1,
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, FlashblocksError::InitialConnection { .. }));
    }

    #[tokio::test]
    async fn hub_broadcast_drops_slow_clients_without_blocking_fast_clients() {
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let hub = Hub::new(metrics);
        let (slow_id, _slow_rx) = hub.register().await;
        let (_fast_id, mut fast_rx) = hub.register().await;
        let slow_tx = hub
            .inner
            .clients
            .lock()
            .await
            .get(&slow_id)
            .unwrap()
            .clone();
        for index in 0..SEND_CHANNEL_BUFFER_SIZE {
            slow_tx
                .try_send(WsPayload(format!("queued-{index}").into_bytes()))
                .unwrap();
        }

        hub.broadcast(WsPayload(b"fresh".to_vec())).await;

        assert_eq!(hub.inner.clients.lock().await.len(), 1);
        assert_eq!(fast_rx.recv().await.unwrap(), WsPayload(b"fresh".to_vec()));
    }

    #[tokio::test]
    async fn downstream_ping_timeout_unregisters_unresponsive_client_like_upstream() {
        let metrics = Arc::new(ConductorMetrics::new("test"));
        let hub = Hub::new(metrics);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/ws", get(handle_fast_timeout_ws))
            .with_state(hub.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let (downstream, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
        let _unpolled_downstream = downstream;

        wait_for_hub_client_count(&hub, 1).await;
        wait_for_hub_client_count(&hub, 0).await;
        server.abort();
    }

    async fn handle_fast_timeout_ws(
        ws: WebSocketUpgrade,
        State(hub): State<Hub>,
    ) -> impl IntoResponse {
        ws.on_upgrade(move |socket| {
            handle_downstream_with_timing(
                socket,
                hub,
                Duration::from_millis(50),
                Duration::from_millis(20),
                Duration::from_millis(50),
            )
        })
    }

    async fn wait_for_hub_client_count(hub: &Hub, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if hub.inner.clients.lock().await.len() == expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    struct TestUpstream {
        url: Url,
        connected: Arc<Notify>,
        send: Arc<Notify>,
        _task: JoinHandle<()>,
    }

    impl TestUpstream {
        async fn start(messages: Vec<&'static str>) -> Self {
            Self::start_frames(messages.into_iter().map(TestFrame::Text).collect()).await
        }

        async fn start_frames(messages: Vec<TestFrame>) -> Self {
            let connected = Arc::new(Notify::new());
            let send = Arc::new(Notify::new());
            let state = TestUpstreamState {
                connected: connected.clone(),
                send: send.clone(),
                messages,
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = Router::new()
                .route("/ws", get(test_upstream_ws))
                .with_state(Arc::new(state));
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                url: format!("ws://{addr}/ws").parse().unwrap(),
                connected,
                send,
                _task: task,
            }
        }

        async fn wait_connected(&self) {
            tokio::time::timeout(Duration::from_secs(2), self.connected.notified())
                .await
                .unwrap();
        }

        fn release_messages(&self) {
            self.send.notify_one();
        }
    }

    struct TestUpstreamState {
        connected: Arc<Notify>,
        send: Arc<Notify>,
        messages: Vec<TestFrame>,
    }

    enum TestFrame {
        Text(&'static str),
        Binary(Vec<u8>),
    }

    async fn test_upstream_ws(
        ws: WebSocketUpgrade,
        State(state): State<Arc<TestUpstreamState>>,
    ) -> impl IntoResponse {
        ws.on_upgrade(move |mut socket| async move {
            state.connected.notify_one();
            state.send.notified().await;
            for message in &state.messages {
                let message = match message {
                    TestFrame::Text(text) => Message::Text((*text).to_string()),
                    TestFrame::Binary(bytes) => Message::Binary(bytes.clone()),
                };
                socket.send(message).await.unwrap();
            }
            while socket.next().await.is_some() {}
        })
    }

    async fn next_text(
        downstream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> String {
        let message = tokio::time::timeout(Duration::from_secs(2), downstream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match message {
            tungstenite::Message::Text(text) => text.to_string(),
            other => panic!("expected text websocket message, got {other:?}"),
        }
    }
}
