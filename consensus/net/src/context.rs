use super::message::ProtocolMsg;
use config::Node;
use futures::channel::mpsc::UnboundedSender;
use std::sync::Arc;
use types::Replica;
use futures::SinkExt;

pub struct Context {
    /// Config context
    /// The number of nodes in the system
    num_nodes: usize,
    /// Sequence Number
    seq: u64,
    /// Network context
    pub net_send: UnboundedSender<(Replica, Arc<ProtocolMsg>)>,
}

impl Context {
    pub fn new(config: &Node, net_send: UnboundedSender<(Replica, Arc<ProtocolMsg>)>) -> Self {
        Context {
            num_nodes: config.num_nodes,
            net_send,
            seq: 0,
        }
    }

    /// Multicast (Sendall) message to all peers
    async fn multicast(&mut self, msg: Arc<ProtocolMsg>) {
        if let Err(e) = self.net_send.send((self.num_nodes(), 
            msg
        )).await {
            log::warn!(
                "Server channel closed with error: {}", e);
        };
    }

    pub(crate) async fn send_next_message(&mut self) {
        let pmsg  =  Arc::new(ProtocolMsg{
            payload: Vec::with_capacity(512),
            seq: self.seq,
        });
        self.seq += 1;
        self.multicast(pmsg).await;
    }

    #[inline]
    pub(crate) fn num_nodes(&self) -> usize {
        self.num_nodes
    }
}
