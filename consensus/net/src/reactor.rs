use super::{context::Context, message::*};
use config::Node;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use std::sync::Arc;
use types::Replica;
use tokio::time::{self, Duration};

/// The reactor reacts to all the messages from the network
pub async fn reactor(
    config: &Node,
    net_send: UnboundedSender<(Replica, Arc<ProtocolMsg>)>,
    mut net_recv: UnboundedReceiver<(Replica, ProtocolMsg)>,
) {
    // Optimization to improve latency when the payloads are high
    // let (send, mut recv) = unbounded_channel();
    let mut cx = Context::new(config, net_send);

    tokio::task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            cx.send_next_message().await;
        }
    });

    loop {
        tokio::select! {
            pmsg_opt = net_recv.next() => {
                // Received a protocol message
                if let None = pmsg_opt {
                    log::error!(
                        "Protocol message channel closed");
                    std::process::exit(0);
                }

                let (sender, pmsg) = pmsg_opt.unwrap();

                log::info!("Received message {} from {}", pmsg.seq, sender);
            },
        }
    }
}
