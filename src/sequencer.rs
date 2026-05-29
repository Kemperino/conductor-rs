use crate::{
    rpc::{ExecutionClient, RollupNodeClient, RpcClientError},
    types::{BlockInfo, Hash, PayloadEnvelope, PeerStats, SyncStatus},
};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum SequencerStartMode {
    HashParam,
    Parameterless,
    Auto,
}

#[derive(Debug, Error)]
pub enum SequencerError {
    #[error("rpc error: {0}")]
    Rpc(#[from] RpcClientError),
    #[error("unsafe head mismatch: expected {expected}, node has {actual} at block {number}")]
    UnsafeHeadMismatch {
        expected: Hash,
        actual: Hash,
        number: u64,
    },
}

impl SequencerError {
    pub fn is_sequencer_already_started(&self) -> bool {
        self.message_contains(&[
            "sequencer already running",
            "sequencer already started",
            "already started",
        ])
    }

    pub fn is_sequencer_already_stopped(&self) -> bool {
        self.message_contains(&[
            "sequencer not running",
            "sequencer already stopped",
            "already stopped",
        ])
    }

    fn message_contains(&self, needles: &[&str]) -> bool {
        let message = match self {
            Self::Rpc(RpcClientError::JsonRpc { message, .. }) => message.as_str(),
            Self::Rpc(RpcClientError::InvalidResponse(message)) => message.as_str(),
            _ => return false,
        };
        needles.iter().any(|needle| message.contains(needle))
    }
}

#[async_trait::async_trait]
pub trait SequencerControl: Send + Sync + std::fmt::Debug {
    async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError>;
    async fn block_by_number(&self, number: u64) -> Result<BlockInfo, SequencerError>;
    async fn start_sequencer(&self, expected_hash: Hash) -> Result<(), SequencerError>;
    async fn stop_sequencer(&self) -> Result<Hash, SequencerError>;
    async fn sequencer_active(&self) -> Result<bool, SequencerError>;
    async fn sync_status(&self) -> Result<SyncStatus, SequencerError>;
    async fn peer_stats(&self) -> Result<PeerStats, SequencerError>;
    async fn post_unsafe_payload(&self, payload: &PayloadEnvelope) -> Result<(), SequencerError>;
    async fn conductor_enabled(&self) -> Result<bool, SequencerError>;
}

#[derive(Debug)]
pub struct SequencerController {
    node: RollupNodeClient,
    execution: ExecutionClient,
    start_mode: SequencerStartMode,
}

impl SequencerController {
    pub fn new(
        node: RollupNodeClient,
        execution: ExecutionClient,
        start_mode: SequencerStartMode,
    ) -> Arc<Self> {
        Arc::new(Self {
            node,
            execution,
            start_mode,
        })
    }

    async fn assert_latest_unsafe(&self, expected_hash: Hash) -> Result<BlockInfo, SequencerError> {
        let block = self.execution.latest_unsafe_block().await?;
        if block.hash != expected_hash {
            return Err(SequencerError::UnsafeHeadMismatch {
                expected: expected_hash,
                actual: block.hash,
                number: block.number,
            });
        }
        Ok(block)
    }

    async fn start_parameterless_after_check(
        &self,
        expected_hash: Hash,
    ) -> Result<(), SequencerError> {
        self.assert_latest_unsafe(expected_hash).await?;
        self.node.start_sequencer_parameterless().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SequencerControl for SequencerController {
    async fn latest_unsafe_block(&self) -> Result<BlockInfo, SequencerError> {
        Ok(self.execution.latest_unsafe_block().await?)
    }

    async fn block_by_number(&self, number: u64) -> Result<BlockInfo, SequencerError> {
        Ok(self.execution.block_by_number(number).await?)
    }

    async fn start_sequencer(&self, expected_hash: Hash) -> Result<(), SequencerError> {
        match self.start_mode {
            SequencerStartMode::HashParam => {
                self.node.start_sequencer_with_hash(expected_hash).await?;
                Ok(())
            }
            SequencerStartMode::Parameterless => {
                self.start_parameterless_after_check(expected_hash).await
            }
            SequencerStartMode::Auto => {
                match self.node.start_sequencer_with_hash(expected_hash).await {
                    Ok(()) => Ok(()),
                    Err(err) if err.is_invalid_params() => {
                        self.start_parameterless_after_check(expected_hash).await
                    }
                    Err(err) => Err(err.into()),
                }
            }
        }
    }

    async fn stop_sequencer(&self) -> Result<Hash, SequencerError> {
        Ok(self.node.stop_sequencer().await?)
    }

    async fn sequencer_active(&self) -> Result<bool, SequencerError> {
        Ok(self.node.sequencer_active().await?)
    }

    async fn sync_status(&self) -> Result<SyncStatus, SequencerError> {
        Ok(self.node.sync_status().await?)
    }

    async fn peer_stats(&self) -> Result<PeerStats, SequencerError> {
        Ok(self.node.peer_stats().await?)
    }

    async fn post_unsafe_payload(&self, payload: &PayloadEnvelope) -> Result<(), SequencerError> {
        Ok(self.node.post_unsafe_payload(payload).await?)
    }

    async fn conductor_enabled(&self) -> Result<bool, SequencerError> {
        Ok(self.node.conductor_enabled().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::net::TcpListener;
    use url::Url;

    #[derive(Debug)]
    struct RpcState {
        unsafe_hash: Hash,
        hash_param_starts: AtomicUsize,
        parameterless_starts: AtomicUsize,
    }

    fn hash(byte: u8) -> Hash {
        format!("0x{}", hex::encode([byte; 32])).parse().unwrap()
    }

    async fn rpc_handler(
        State(state): State<Arc<RpcState>>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        let method = request.get("method").and_then(Value::as_str).unwrap();
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        match method {
            "admin_startSequencer" => {
                let params = request
                    .get("params")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if params.is_empty() {
                    state.parameterless_starts.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"jsonrpc": "2.0", "id": id, "result": null}))
                } else {
                    state.hash_param_starts.fetch_add(1, Ordering::SeqCst);
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32602,
                            "message": "invalid params"
                        }
                    }))
                }
            }
            "eth_getBlockByNumber" => Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "hash": state.unsafe_hash.to_string(),
                    "number": "0xa"
                }
            })),
            _ => Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": "method not found"
                }
            })),
        }
    }

    async fn test_controller(unsafe_hash: Hash) -> (Arc<RpcState>, SequencerController) {
        let state = Arc::new(RpcState {
            unsafe_hash,
            hash_param_starts: AtomicUsize::new(0),
            parameterless_starts: AtomicUsize::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", post(rpc_handler))
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url = Url::parse(&format!("http://{addr}")).unwrap();
        (
            state,
            SequencerController {
                node: RollupNodeClient::new(url.clone()),
                execution: ExecutionClient::new(url),
                start_mode: SequencerStartMode::Auto,
            },
        )
    }

    #[tokio::test]
    async fn auto_mode_falls_back_to_kona_parameterless_start_after_hash_check() {
        let expected = hash(0x10);
        let (state, controller) = test_controller(expected).await;

        controller.start_sequencer(expected).await.unwrap();

        assert_eq!(state.hash_param_starts.load(Ordering::SeqCst), 1);
        assert_eq!(state.parameterless_starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn auto_mode_refuses_parameterless_start_when_unsafe_head_mismatches() {
        let expected = hash(0x10);
        let (state, controller) = test_controller(hash(0x11)).await;

        let err = controller.start_sequencer(expected).await.unwrap_err();

        assert!(matches!(err, SequencerError::UnsafeHeadMismatch { .. }));
        assert_eq!(state.hash_param_starts.load(Ordering::SeqCst), 1);
        assert_eq!(state.parameterless_starts.load(Ordering::SeqCst), 0);
    }
}
