use crate::types::PayloadEnvelope;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("payload error: {0}")]
    Payload(#[from] crate::types::TypeError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[async_trait::async_trait]
pub trait PayloadStore: Send + Sync + std::fmt::Debug {
    async fn commit(&self, payload: PayloadEnvelope) -> Result<(), StoreError>;
    async fn latest(&self) -> Result<Option<PayloadEnvelope>, StoreError>;
}

#[derive(Debug)]
pub struct FilePayloadStore {
    path: PathBuf,
    latest: RwLock<Option<PayloadEnvelope>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedPayload {
    version: u8,
    payload: PayloadEnvelope,
}

impl FilePayloadStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Arc<Self>, StoreError> {
        let path = path.as_ref().to_path_buf();
        let latest = match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let persisted: PersistedPayload = serde_json::from_slice(&bytes)?;
                Some(persisted.payload)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };

        Ok(Arc::new(Self {
            path,
            latest: RwLock::new(latest),
        }))
    }

    async fn persist(&self, payload: &PayloadEnvelope) -> Result<(), StoreError> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(&PersistedPayload {
            version: 1,
            payload: payload.clone(),
        })?;

        {
            let file = tokio::fs::File::create(&tmp).await?;
            file.set_len(0).await?;
            tokio::fs::write(&tmp, bytes).await?;
            let file = tokio::fs::OpenOptions::new().read(true).open(&tmp).await?;
            file.sync_all().await?;
        }

        tokio::fs::rename(&tmp, &self.path).await?;
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PayloadStore for FilePayloadStore {
    async fn commit(&self, payload: PayloadEnvelope) -> Result<(), StoreError> {
        let new_number = payload.block_number()?;
        payload.block_hash()?;
        let mut latest = self.latest.write().await;
        if let Some(current) = latest.as_ref() {
            if current.block_number()? >= new_number {
                return Ok(());
            }
        }

        self.persist(&payload).await?;
        *latest = Some(payload);
        Ok(())
    }

    async fn latest(&self) -> Result<Option<PayloadEnvelope>, StoreError> {
        Ok(self.latest.read().await.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(number: u64, byte: u8) -> PayloadEnvelope {
        PayloadEnvelope::new(serde_json::json!({
            "executionPayload": {
                "blockHash": format!("0x{}", hex::encode([byte; 32])),
                "blockNumber": format!("0x{number:x}")
            }
        }))
    }

    fn payload_with_hash(number: u64, hash: &str) -> PayloadEnvelope {
        PayloadEnvelope::new(serde_json::json!({
            "executionPayload": {
                "blockHash": hash,
                "blockNumber": format!("0x{number:x}")
            }
        }))
    }

    #[tokio::test]
    async fn persists_latest_payload_monotonically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unsafe.json");
        let store = FilePayloadStore::open(&path).await.unwrap();

        store.commit(payload(10, 0x10)).await.unwrap();
        store.commit(payload(9, 0x09)).await.unwrap();
        store.commit(payload(10, 0x11)).await.unwrap();

        let latest = store.latest().await.unwrap().unwrap();
        assert_eq!(latest.block_number().unwrap(), 10);
        assert_eq!(
            latest.block_hash().unwrap().to_string(),
            format!("0x{}", hex::encode([0x10; 32]))
        );

        let reopened = FilePayloadStore::open(path).await.unwrap();
        let latest = reopened.latest().await.unwrap().unwrap();
        assert_eq!(
            latest.block_hash().unwrap().to_string(),
            format!("0x{}", hex::encode([0x10; 32]))
        );
    }

    #[tokio::test]
    async fn rejects_invalid_payload_hash_before_persisting_like_upstream() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unsafe.json");
        let store = FilePayloadStore::open(&path).await.unwrap();

        let err = store
            .commit(payload_with_hash(10, "0x1234"))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("expected 0x-prefixed 32-byte hash"));
        assert!(store.latest().await.unwrap().is_none());
        assert!(!path.exists());
    }
}
