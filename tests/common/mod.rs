use conductor_rs::Hash;
use serde_json::{json, Value};

pub fn hash(byte: u8) -> Hash {
    format!("0x{}", hex::encode([byte; 32])).parse().unwrap()
}

pub fn kona_payload_hash() -> Hash {
    // Header hash for the static V2 execution payload built by `kona_payload`.
    "0xcccac6485e2fed30edd886db7be151bb3e80694b4f6115746a8a92832caf9047"
        .parse()
        .unwrap()
}

pub fn kona_payload(number: u64, block_hash: Hash) -> Value {
    json!({
        "executionPayload": {
            "parentHash": hash(0x01).to_string(),
            "feeRecipient": "0x0000000000000000000000000000000000000000",
            "stateRoot": hash(0x02).to_string(),
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "prevRandao": hash(0x04).to_string(),
            "blockNumber": format!("0x{number:x}"),
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x0",
            "timestamp": "0x1",
            "extraData": "0x",
            "baseFeePerGas": "0x0",
            "blockHash": block_hash.to_string(),
            "transactions": [],
            "withdrawals": []
        }
    })
}

pub fn validate_kona_payload_shape(raw: &Value) -> Result<(), String> {
    let payload = raw
        .get("executionPayload")
        .ok_or_else(|| "payload is missing executionPayload".to_string())?;
    for field in [
        "parentHash",
        "feeRecipient",
        "stateRoot",
        "receiptsRoot",
        "logsBloom",
        "prevRandao",
        "blockNumber",
        "gasLimit",
        "gasUsed",
        "timestamp",
        "extraData",
        "baseFeePerGas",
        "blockHash",
        "transactions",
        "withdrawals",
    ] {
        if payload.get(field).is_none() {
            return Err(format!("payload is missing executionPayload.{field}"));
        }
    }
    if !payload["transactions"].is_array() {
        return Err("executionPayload.transactions must be an array".to_string());
    }
    if !payload["withdrawals"].is_array() {
        return Err("executionPayload.withdrawals must be an array".to_string());
    }
    Ok(())
}
