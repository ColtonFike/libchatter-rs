use super::{context::Context, message::*};
use config::Node;
/// The core consensus module used for Apollo
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use futures::channel::mpsc::{unbounded as unbounded_channel, UnboundedReceiver, UnboundedSender};
use futures::StreamExt;
use std::sync::Arc;
use types::Replica;
use std::time::Instant;

pub async fn reactor(
    config:&Node,
    net_send: UnboundedSender<(Replica, Arc<ProtocolMsg>)>,
    mut net_recv: UnboundedReceiver<(Replica, ProtocolMsg)>,
) {
    // Optimization to improve latency when the payloads are high
    // let (send, mut recv) = unbounded_channel();
    let mut cx = Context::new(config, net_send);

    let myid = config.id;
    let mut count: u32 = 0;
    let mut now = Instant::now();
    let mut total: u128 = 0;
    let mut iter: u32 = 0;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    if myid == 0 {
        let payload: [u8; 500] = [0; 500];
        let msg = ProtocolMsg{ payload: payload.to_vec() };
        now = Instant::now();
        cx.multicast(Arc::new(msg)).await;
    }
    loop {
        tokio::select! {
            pmsg_opt = net_recv.next() => {
                // Received a protocol message
                if let None = pmsg_opt {
                    log::error!(
                        "Protocol message channel closed");
                    std::process::exit(0);
                }
                count += 1;
                let (sender, pmsg) = pmsg_opt.unwrap();
                if sender == 0 {
                    cx.multicast(Arc::new(pmsg)).await;
                }
                if myid == 0 && count >= (cx.num_nodes() - 1) as u32 {
                    count = 0;
                    iter += 1;
                    let payload: [u8; 500] = [0; 500];
                    let msg = ProtocolMsg{ payload: payload.to_vec() };
                    total += now.elapsed().as_micros();
                    println!("Average Time: {}", total as f64 / iter as f64);
                    now = Instant::now();
                    cx.multicast(Arc::new(msg)).await;
                }
            },
        }
    }
}
