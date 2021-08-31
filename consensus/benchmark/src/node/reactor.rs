//use super::{context::Context, message::*, proposal::*};
use config::Node;
/// The core consensus module used for Apollo
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use futures::channel::mpsc::{unbounded as unbounded_channel, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;

pub async fn reactor(
    config:&Node,
    is_client_apollo_enabled: bool,
    net_send: UnboundedSender<(&[u8], &[u8])>,
    mut net_recv: UnboundedReceiver<(&[u8], &[u8])>,
) {
    // Optimization to improve latency when the payloads are high
    //let (send, mut recv) = unbounded_channel();

    let block_size = config.block_size;
    let myid = config.id;
    let pl_size = config.payload;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
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
            },
        }
    }
}
