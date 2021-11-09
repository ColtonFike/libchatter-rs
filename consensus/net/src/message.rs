use serde::{Deserialize, Serialize};
use types::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProtocolMsg {
    pub seq: u64,
    pub payload: Vec<u8>,
}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self = bincode::deserialize(bytes).expect("Failed to decode the protocol message");
        c.init()
    }

    fn init(self) -> Self {
        match self {
            _x => _x,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let bytes = bincode::serialize(self).expect("Failed to serialize protocol message");
        bytes
    }
}
