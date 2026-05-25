use crate::{store::PayloadStore, types::PayloadEnvelope};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("node is not the leader")]
    NotLeader,
    #[error("server {0} not found")]
    ServerNotFound(String),
    #[error("server {id} has addr {actual}, not requested addr {expected}")]
    ServerAddrMismatch {
        id: String,
        expected: String,
        actual: String,
    },
    #[error("configuration changed since {expected} (latest is {actual})")]
    VersionMismatch { expected: u64, actual: u64 },
    #[error("invalid unsafe payload: {0}")]
    InvalidPayload(String),
    #[error("raft error: {0}")]
    Raft(String),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
}

impl ConsensusError {
    pub fn is_leadership_transfer_in_progress(&self) -> bool {
        match self {
            Self::Raft(message) => message.contains("leadership transfer already in progress"),
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerSuffrage {
    Voter,
    Nonvoter,
}

impl ServerSuffrage {
    fn as_u8(self) -> u8 {
        match self {
            Self::Voter => 0,
            Self::Nonvoter => 1,
        }
    }
}

impl Serialize for ServerSuffrage {
    fn serialize<T>(&self, serializer: T) -> Result<T::Ok, T::Error>
    where
        T: Serializer,
    {
        serializer.serialize_u8(self.as_u8())
    }
}

impl<'de> Deserialize<'de> for ServerSuffrage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u8::deserialize(deserializer)? {
            0 => Ok(Self::Voter),
            1 => Ok(Self::Nonvoter),
            other => Err(serde::de::Error::custom(format!(
                "invalid server suffrage {other}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServerInfo {
    pub id: String,
    pub addr: String,
    pub suffrage: ServerSuffrage,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterMembership {
    pub servers: Vec<ServerInfo>,
    pub version: u64,
}

#[async_trait::async_trait]
pub trait Consensus: Send + Sync + std::fmt::Debug {
    fn addr(&self) -> String;
    fn server_id(&self) -> &str;
    fn leader(&self) -> bool;
    fn set_leader_for_tests(&self, leader: bool);
    fn leader_with_id(&self) -> ServerInfo;
    async fn add_voter(&self, id: String, addr: String, version: u64)
        -> Result<(), ConsensusError>;
    async fn add_non_voter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConsensusError>;
    async fn demote_voter(&self, id: String, version: u64) -> Result<(), ConsensusError>;
    async fn remove_server(&self, id: String, version: u64) -> Result<(), ConsensusError>;
    async fn membership(&self) -> Result<ClusterMembership, ConsensusError>;
    async fn transfer_leader(&self) -> Result<(), ConsensusError>;
    async fn transfer_leader_to(&self, id: String, addr: String) -> Result<(), ConsensusError>;
    async fn commit_unsafe_payload(&self, payload: PayloadEnvelope) -> Result<(), ConsensusError>;
    async fn latest_unsafe_payload(&self) -> Result<Option<PayloadEnvelope>, ConsensusError>;
    async fn shutdown(&self) -> Result<(), ConsensusError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct LocalConsensus<S> {
    server_id: String,
    addr: String,
    leader: AtomicBool,
    members: RwLock<BTreeMap<String, ServerInfo>>,
    version: AtomicU64,
    store: Arc<S>,
}

impl<S> LocalConsensus<S> {
    pub fn new(
        server_id: impl Into<String>,
        addr: impl Into<String>,
        leader: bool,
        store: Arc<S>,
    ) -> Self {
        let server_id = server_id.into();
        let addr = addr.into();
        let mut members = BTreeMap::new();
        members.insert(
            server_id.clone(),
            ServerInfo {
                id: server_id.clone(),
                addr: addr.clone(),
                suffrage: ServerSuffrage::Voter,
            },
        );

        Self {
            server_id,
            addr,
            leader: AtomicBool::new(leader),
            members: RwLock::new(members),
            version: AtomicU64::new(1),
            store,
        }
    }

    fn check_version(&self, expected: u64) -> Result<(), ConsensusError> {
        if expected == 0 {
            return Ok(());
        }
        let actual = self.version.load(Ordering::SeqCst);
        if actual != expected {
            return Err(ConsensusError::VersionMismatch { expected, actual });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<S> Consensus for LocalConsensus<S>
where
    S: PayloadStore + 'static,
{
    fn addr(&self) -> String {
        self.addr.clone()
    }

    fn server_id(&self) -> &str {
        &self.server_id
    }

    fn leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    fn set_leader_for_tests(&self, leader: bool) {
        self.leader.store(leader, Ordering::SeqCst);
    }

    fn leader_with_id(&self) -> ServerInfo {
        ServerInfo {
            id: self.server_id.clone(),
            addr: self.addr.clone(),
            suffrage: ServerSuffrage::Voter,
        }
    }

    async fn add_voter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConsensusError> {
        self.check_version(version)?;
        self.members.write().await.insert(
            id.clone(),
            ServerInfo {
                id,
                addr,
                suffrage: ServerSuffrage::Voter,
            },
        );
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn add_non_voter(
        &self,
        id: String,
        addr: String,
        version: u64,
    ) -> Result<(), ConsensusError> {
        self.check_version(version)?;
        self.members.write().await.insert(
            id.clone(),
            ServerInfo {
                id,
                addr,
                suffrage: ServerSuffrage::Nonvoter,
            },
        );
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn demote_voter(&self, id: String, version: u64) -> Result<(), ConsensusError> {
        self.check_version(version)?;
        let mut members = self.members.write().await;
        let member = members
            .get_mut(&id)
            .ok_or_else(|| ConsensusError::ServerNotFound(id.clone()))?;
        member.suffrage = ServerSuffrage::Nonvoter;
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn remove_server(&self, id: String, version: u64) -> Result<(), ConsensusError> {
        self.check_version(version)?;
        self.members.write().await.remove(&id);
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn membership(&self) -> Result<ClusterMembership, ConsensusError> {
        Ok(ClusterMembership {
            servers: self.members.read().await.values().cloned().collect(),
            version: self.version.load(Ordering::SeqCst),
        })
    }

    async fn transfer_leader(&self) -> Result<(), ConsensusError> {
        self.leader.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn transfer_leader_to(&self, id: String, addr: String) -> Result<(), ConsensusError> {
        let members = self.members.read().await;
        let member = members
            .get(&id)
            .ok_or_else(|| ConsensusError::ServerNotFound(id.clone()))?;
        if member.addr != addr {
            return Err(ConsensusError::ServerAddrMismatch {
                id,
                expected: addr,
                actual: member.addr.clone(),
            });
        }
        self.leader.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn commit_unsafe_payload(&self, payload: PayloadEnvelope) -> Result<(), ConsensusError> {
        if !self.leader() {
            return Err(ConsensusError::NotLeader);
        }
        self.store.commit(payload).await?;
        Ok(())
    }

    async fn latest_unsafe_payload(&self) -> Result<Option<PayloadEnvelope>, ConsensusError> {
        Ok(self.store.latest().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FilePayloadStore;

    async fn local_consensus() -> LocalConsensus<FilePayloadStore> {
        let dir = tempfile::tempdir().unwrap();
        let store = FilePayloadStore::open(dir.path().join("unsafe.json"))
            .await
            .unwrap();
        LocalConsensus::new("seq-a", "127.0.0.1:1001", true, store)
    }

    #[tokio::test]
    async fn local_membership_changes_enforce_nonzero_version() {
        let consensus = local_consensus().await;

        let err = consensus
            .add_voter("seq-b".to_string(), "127.0.0.1:1002".to_string(), 99)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ConsensusError::VersionMismatch {
                expected: 99,
                actual: 1
            }
        ));
        assert!(err.to_string().contains("configuration changed since"));
        assert_eq!(consensus.membership().await.unwrap().servers.len(), 1);

        consensus
            .add_voter("seq-b".to_string(), "127.0.0.1:1002".to_string(), 1)
            .await
            .unwrap();
        assert_eq!(consensus.membership().await.unwrap().version, 2);

        let err = consensus
            .remove_server("seq-b".to_string(), 1)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ConsensusError::VersionMismatch {
                expected: 1,
                actual: 2
            }
        ));
        assert!(err.to_string().contains("configuration changed since"));
    }

    #[test]
    fn consensus_error_strings_match_upstream_e2e_substrings() {
        assert_eq!(
            ConsensusError::NotLeader.to_string(),
            "node is not the leader"
        );
        assert!(ConsensusError::VersionMismatch {
            expected: 1,
            actual: 2
        }
        .to_string()
        .contains("configuration changed since"));
    }

    #[tokio::test]
    async fn local_membership_version_zero_matches_upstream_unchecked_change() {
        let consensus = local_consensus().await;

        consensus
            .add_non_voter("seq-b".to_string(), "127.0.0.1:1002".to_string(), 0)
            .await
            .unwrap();

        let membership = consensus.membership().await.unwrap();
        assert_eq!(membership.version, 2);
        assert_eq!(
            membership
                .servers
                .iter()
                .find(|server| server.id == "seq-b")
                .unwrap()
                .suffrage,
            ServerSuffrage::Nonvoter
        );
    }

    #[tokio::test]
    async fn local_transfer_leader_to_requires_matching_addr_like_upstream() {
        let consensus = local_consensus().await;
        consensus
            .add_voter("seq-b".to_string(), "127.0.0.1:1002".to_string(), 1)
            .await
            .unwrap();

        let err = consensus
            .transfer_leader_to("seq-b".to_string(), "127.0.0.1:9999".to_string())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ConsensusError::ServerAddrMismatch {
                id,
                expected,
                actual
            } if id == "seq-b"
                && expected == "127.0.0.1:9999"
                && actual == "127.0.0.1:1002"
        ));
        assert!(consensus.leader());
    }
}
