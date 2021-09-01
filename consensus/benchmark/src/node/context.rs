use super::message::ProtocolMsg;
use config::Node;
use crypto::{ed25519, secp256k1, Keypair, PublicKey};
use fnv::FnvHashMap as HashMap;
use futures::channel::mpsc::UnboundedSender;
use std::sync::Arc;
use types::Replica;

pub struct Context {
    /// Config context
    /// The number of nodes in the system
    num_nodes: usize,
    /// The number of faults in the system
    num_faults: usize,
    /// My ID
    myid: Replica,
    /// Everyone's public keys
    pub pub_key_map: HashMap<Replica, PublicKey>,
    /// My key
    pub my_secret_key: Arc<Keypair>,
    /// Whether the client supports Apollo or not

    /// Network context
    pub net_send: UnboundedSender<(Replica, Arc<ProtocolMsg>)>,
}

const EXTRA_SPACE: usize = 100;

impl Context {
    pub fn new(config: &Node, net_send: UnboundedSender<(Replica, Arc<ProtocolMsg>)>) -> Self {
        let mut c = Context {
            num_nodes: config.num_nodes,
            num_faults: config.num_faults,
            myid: config.id,
            my_secret_key: match config.crypto_alg {
                crypto::Algorithm::ED25519 => {
                    let mut sk_copy = config.secret_key_bytes.clone();
                    let kp = ed25519::Keypair::decode(&mut sk_copy)
                        .expect("Failed to decode the secret key from the config");
                    Arc::new(Keypair::Ed25519(kp))
                }
                crypto::Algorithm::SECP256K1 => {
                    let sk_copy = config.secret_key_bytes.clone();
                    let sk = secp256k1::SecretKey::from_bytes(sk_copy)
                        .expect("Failed to decode the secret key from the config");
                    let kp = secp256k1::Keypair::from(sk);
                    Arc::new(Keypair::Secp256k1(kp))
                }
                _ => panic!("Unimplemented algorithm"),
            },
            pub_key_map: HashMap::default(),
            net_send,
        };
        for (id, mut pk_data) in &config.pk_map {
            if *id == c.myid {
                continue;
            }
            let pk = match config.crypto_alg {
                crypto::Algorithm::ED25519 => {
                    let kp = ed25519::PublicKey::decode(&mut pk_data)
                        .expect("Failed to decode the secret key from the config");
                    PublicKey::Ed25519(kp)
                }
                crypto::Algorithm::SECP256K1 => {
                    let sk = secp256k1::PublicKey::decode(&pk_data)
                        .expect("Failed to decode the secret key from the config");
                    PublicKey::Secp256k1(sk)
                }
                _ => panic!("Unimplemented algorithm"),
            };
            c.pub_key_map.insert(*id, pk);
        }
        c
    }

    #[inline]
    pub(crate) fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline]
    pub(crate) fn num_faults(&self) -> usize {
        self.num_faults
    }

    #[inline]
    pub(crate) fn myid(&self) -> Replica {
        self.myid
    }

    pub(crate) fn next_of(&self, prev: Replica) -> Replica {
        if prev + 1 == self.num_nodes {
            0 as Replica
        } else {
            prev + 1
        }
    }
}
