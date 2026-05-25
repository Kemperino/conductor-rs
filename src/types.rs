use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::{fmt, str::FromStr};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TypeError {
    #[error("expected 0x-prefixed 32-byte hash, got {0}")]
    InvalidHash(String),
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("invalid block number {0}")]
    InvalidBlockNumber(String),
    #[error("payload is missing executionPayload")]
    MissingExecutionPayload,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash([u8; 32]);

impl Hash {
    pub const ZERO: Self = Self([0; 32]);

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl FromStr for Hash {
    type Err = TypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let stripped = value
            .strip_prefix("0x")
            .ok_or_else(|| TypeError::InvalidHash(value.to_string()))?;
        if stripped.len() != 64 {
            return Err(TypeError::InvalidHash(value.to_string()));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(stripped, &mut bytes)
            .map_err(|_| TypeError::InvalidHash(value.to_string()))?;
        Ok(Self(bytes))
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockInfo {
    #[serde(rename = "hash")]
    pub hash: Hash,
    #[serde(rename = "number")]
    pub number: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2BlockRef {
    #[serde(default)]
    pub hash: Option<Hash>,
    #[serde(deserialize_with = "deserialize_quantity")]
    pub number: u64,
    #[serde(
        rename = "timestamp",
        alias = "time",
        deserialize_with = "deserialize_quantity"
    )]
    pub time: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStatus {
    #[serde(rename = "unsafe_l2", alias = "unsafeL2")]
    pub unsafe_l2: L2BlockRef,
    #[serde(rename = "safe_l2", alias = "safeL2")]
    pub safe_l2: L2BlockRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStats {
    #[serde(
        rename = "connected",
        alias = "Connected",
        deserialize_with = "deserialize_quantity"
    )]
    pub connected: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PayloadEnvelope {
    #[serde(flatten)]
    raw: Value,
}

impl PayloadEnvelope {
    pub fn new(raw: Value) -> Self {
        Self { raw }
    }

    pub fn raw(&self) -> &Value {
        &self.raw
    }

    pub fn into_raw(self) -> Value {
        self.raw
    }

    pub fn block_hash(&self) -> Result<Hash, TypeError> {
        let payload = self.execution_payload()?;
        let raw = payload
            .get("blockHash")
            .or_else(|| payload.get("block_hash"))
            .and_then(Value::as_str)
            .ok_or(TypeError::MissingField("executionPayload.blockHash"))?;
        raw.parse()
    }

    pub fn block_number(&self) -> Result<u64, TypeError> {
        let payload = self.execution_payload()?;
        let value = payload
            .get("blockNumber")
            .or_else(|| payload.get("block_number"))
            .ok_or(TypeError::MissingField("executionPayload.blockNumber"))?;
        parse_quantity(value)
    }

    fn execution_payload(&self) -> Result<&Value, TypeError> {
        self.raw
            .get("executionPayload")
            .or_else(|| self.raw.get("execution_payload"))
            .ok_or(TypeError::MissingExecutionPayload)
    }
}

pub fn parse_quantity(value: &Value) -> Result<u64, TypeError> {
    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| TypeError::InvalidBlockNumber(value.to_string())),
        Value::String(s) => {
            if let Some(hex) = s.strip_prefix("0x") {
                u64::from_str_radix(hex, 16)
                    .map_err(|_| TypeError::InvalidBlockNumber(s.to_string()))
            } else {
                s.parse()
                    .map_err(|_| TypeError::InvalidBlockNumber(s.to_string()))
            }
        }
        _ => Err(TypeError::InvalidBlockNumber(value.to_string())),
    }
}

fn deserialize_quantity<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    parse_quantity(&value).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> String {
        format!("0x{}", hex::encode([byte; 32]))
    }

    #[test]
    fn extracts_camel_case_payload_fields() {
        let payload = PayloadEnvelope::new(serde_json::json!({
            "executionPayload": {
                "blockHash": hash(0x11),
                "blockNumber": "0x2a"
            }
        }));

        assert_eq!(payload.block_hash().unwrap().to_string(), hash(0x11));
        assert_eq!(payload.block_number().unwrap(), 42);
    }

    #[test]
    fn rejects_short_hashes() {
        assert!("0x1234".parse::<Hash>().is_err());
    }

    #[test]
    fn decodes_sync_status_block_refs() {
        let status: SyncStatus = serde_json::from_value(serde_json::json!({
            "unsafe_l2": {
                "hash": hash(0x22),
                "number": "0x2a",
                "timestamp": "0x64"
            },
            "safe_l2": {
                "number": 40,
                "time": 90
            }
        }))
        .unwrap();

        assert_eq!(status.unsafe_l2.number, 42);
        assert_eq!(status.unsafe_l2.time, 100);
        assert_eq!(status.safe_l2.number, 40);
        assert_eq!(status.safe_l2.time, 90);
    }

    #[test]
    fn decodes_peer_stats_connected_count() {
        let stats: PeerStats =
            serde_json::from_value(serde_json::json!({"connected": "0x3"})).unwrap();

        assert_eq!(stats.connected, 3);
    }
}
