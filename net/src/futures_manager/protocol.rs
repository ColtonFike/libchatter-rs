use std::{
    sync::Arc,
    pin::Pin,
};
use fnv::FnvHashMap as HashMap;
use tokio::{
    net::{
        TcpListener, 
        TcpStream
    }, 
};
use futures::{
    channel::mpsc::{
        UnboundedSender,
        UnboundedReceiver,
        unbounded as unbounded_channel,
    },
    SinkExt,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{
    Decoder, 
    Encoder, 
};
use types::{
    Replica, 
    WireReady
};
use futures::Stream;
use tokio_stream::{StreamMap, StreamExt};
use super::peer::Peer;
use super::connection::{ConnectionManager, Function};

use super::Protocol;

type Err = std::io::Error;

impl<I,O> Protocol<I,O>
where I:WireReady + Send + Sync + 'static + Unpin,
O:WireReady + Clone + Sync + 'static + Unpin, 
{
    pub async fn server_setup(
        &self,
        node_addr: HashMap<Replica, String>, 
        enc: impl Encoder<Arc<O>> + Clone + Send + Sync + 'static, 
        dec: impl Decoder<Item=I, Error=Err> + Clone + Send + Sync + 'static
    ) -> (UnboundedSender<(Replica, Arc<O>)>, UnboundedReceiver<(Replica, I)>)
    {
        let (new_connections_tx, new_connections_rx) = unbounded_channel::<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>();
        let function_caller = ConnectionManager::new(self.my_id, node_addr[&self.my_id].clone(), node_addr.clone(), new_connections_tx, dec, enc).await;

        // Create a unified reader stream
        let unified_stream = StreamMap::new();

        // Create write end points for peers
        let writer_end_points = HashMap::default();
        for i in 0..self.num_nodes {
            if i == self.my_id {
                continue;
            }
        }

        // Create channels so that the outside world can communicate with the
        // network
        let (in_send, in_recv) = unbounded_channel::<(Replica, I)>();
        let (out_send, out_recv) = unbounded_channel();

        // Start the event loop that processes network messages
        tokio::spawn(
            protocol_event_loop(
                self.num_nodes, 
                in_send, 
                out_recv, 
                unified_stream, 
                writer_end_points,
                function_caller,
                new_connections_rx,
            )
        );
    
        (out_send, in_recv)
    }

    pub async fn client_setup(
        &self,
        listen: String,
        enc: impl Encoder<Arc<O>> + Clone + Send + Sync + 'static, 
        dec: impl Decoder<Item=I, Error=Err> + Clone + Send + Sync + 'static
    ) -> (UnboundedSender<Arc<O>>, UnboundedReceiver<I>) 
    {
        let (cli_in_send, cli_in_recv) = unbounded_channel();
        let (cli_out_send, cli_out_recv) = unbounded_channel();
        
        let cli_manager_stream = cli_manager(listen).await;
        tokio::spawn(
            client_event_loop(enc, dec, cli_out_recv, cli_in_send, cli_manager_stream, self.cli_acceptor.clone())
        );
        (cli_out_send, cli_in_recv)
    }
}

async fn protocol_event_loop<I,O>(
    num_nodes: Replica, 
    mut in_send: UnboundedSender<(Replica, I)>,
    mut out_recv: UnboundedReceiver<(Replica, Arc<O>)>,
    mut reading_net: StreamMap<Replica, UnboundedReceiver<I>>,
    mut writers: HashMap<Replica, UnboundedSender<Arc<O>>>,
    mut function_caller: UnboundedSender<Function>,
    mut new_connections: UnboundedReceiver<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
)
where I:WireReady + Send + Sync + 'static + Unpin,
O:WireReady + Clone + Sync + 'static + Unpin, 
{
    let mut all_connected = false;
    loop {
        tokio::select!{
            new_connection = new_connections.next() => {
                if let None = new_connection {
                    log::info!("Internal connection manager closed");
                    std::process::exit(0);
                }
                let (id, writer, reader) = new_connection.unwrap();
                writers.insert(id, writer);
                reading_net.insert(id, reader);
                all_connected = true;
                log::info!("Successfully connected to peer {}", id);
            },
            opt_in = reading_net.next() => {
                if let None = opt_in {
                    // log::error!(
                        // "Failed to read a protocol message from a peer");

                    // TODO this error occurs when no peers are connected
                    // How should that situation be handled?
                    if all_connected {
                        // TODO reading_net.next returns none because streams are closed look into
                        // this as souce of bug
                        writers.clear();
                        reading_net.clear();
                        log::info!("All nodes disconnected!");
                        function_caller.send(Function::AllDisconnected).await.unwrap();
                        log::info!("Message sent!");
                        all_connected = false;
                    }
                    // continue;
                    // std::process::exit(0);
                } else {
                    let (id, msg) = opt_in.unwrap();
                    if let Err(e) = in_send.send((id, msg.init())).await {
                        // TODO add disconnected nodes here as well
                        log::error!(
                            "Failed to send a protocol message outside the network, with error {}", e);
                        //std::process::exit(0);
                    }
                }
            },
            opt_out = out_recv.next() => {
                if let None = opt_out {
                    log::error!(
                        "Failed to read a protocol message to send outside the network");
                    //std::process::exit(0);
                }
                let (to, msg) = opt_out.unwrap();
                if to < num_nodes {
                    if let Err(_e) = writers[&to].clone().send(msg).await {
                        // TODO add disconnected nodes here as well
                        log::error!("Failed to send msg to peer {}", to);
                        //std::process::exit(0);
                    }
                } else {
                    let mut disconnected = Vec::new();

                    // TODO how to not use clone here?
                    for (id, writer) in writers.clone() {
                        if let Err(e) = writer.clone().send(msg.clone()).await {
                            log::error!("Failed to send msg to peer {} with error {}", id, e);
                            disconnected.push(id);
                        }
                    }

                    for id in disconnected {
                        writers.remove(&id);
                        reading_net.remove(&id);
                        function_caller.send(Function::AddConnection(id, 10)).await.unwrap();
                    }
                }
            },
        }
    }
}

async fn cli_manager(addr: String) -> UnboundedReceiver<TcpStream> {
    // Wait for new connections
    println!("Listening for client connections!");
    let cli_sock = TcpListener::bind(addr)
        .await
        .expect("Failed to listen to client connections");
    println!("Success Listening for client connections!");

    // Create channels to let the world know that we have a new client
    // connection
    let (mut conn_ch_send, conn_ch_recv) = unbounded_channel();
    tokio::spawn(async move {
        loop {
            let conn_opt = cli_sock.accept().await;
            let conn = match conn_opt {
                Err(e) => {
                    log::error!("Failed to accept a connection from the client with error {}", e);
                    continue;
                },
                Ok((conn, from)) => {
                    if let Err(e) = conn.set_nodelay(true) {
                        log::error!("Failed to set high speed socket for client: {} with error {}", from, e);
                        continue;
                    }
                    conn
                }
            };
            if let Err(e) = conn_ch_send.send(conn).await {
                log::error!("Failed to send out new client connection: {}", e);
                std::process::exit(0);
            }
        }
    });
    conn_ch_recv
}

async fn client_event_loop<I,O>(
    enc: impl Encoder<Arc<O>> + Clone + Send + Sync + 'static, 
    dec: impl Decoder<Item=I, Error=Err> + Clone + Send + Sync + 'static,
    mut send_out_ch: UnboundedReceiver<Arc<O>>,
    mut new_in_ch: UnboundedSender<I>,
    mut new_conn_ch: UnboundedReceiver<TcpStream>,
    cli_acceptor: TlsAcceptor
) where I:WireReady + Sync + Unpin + 'static,
O: WireReady + Clone+Unpin+Sync + 'static,
{
    let mut read_stream:StreamMap<usize, Pin<Box<dyn Stream<Item=I>+Send>>> = StreamMap::new();
    let mut client_id = 0 as usize;
    let mut writers = HashMap::default();
    let mut to_remove = Vec::new();
    loop {
        tokio::select! {
            // We received something from the client
            in_opt = read_stream.next(), if read_stream.len() > 0 => {
                if let None = in_opt {
                    log::warn!("Read stream closed");
                    std::process::exit(0);
                }
                let (_id, msg) = in_opt.unwrap();
                let msg = msg.init();
                if let Err(e) = new_in_ch.send(msg).await {
                    log::error!("Failed to send an incoming client message outside, with error {}", e);
                    std::process::exit(0);
                }
            },
            // We have a new client
            conn_opt = new_conn_ch.next() => {
                if let None = conn_opt {
                    log::warn!("New connection channel closed");
                    std::process::exit(0);
                }
                let conn = conn_opt.unwrap();
                let new_acceptor = cli_acceptor.clone();
                let conn = new_acceptor.accept(conn).await.unwrap();
                let (read, write) = tokio::io::split(conn);
                let client_peer = Peer::new(read, write, dec.clone(), enc.clone());
                let client_recv = client_peer.recv;
                read_stream.insert(
                    client_id, 
                // Box::pin(async_stream::stream! {
                //     while let Some(item) = client_recv.recv().await {
                //         yield(item);
                //     }
                // }) as std::pin::Pin<Box<dyn futures_util::stream::Stream<Item=I> +Send>>
                    Box::pin(client_recv)
                );
                writers.insert(client_id, client_peer.send);

                client_id = client_id + 1;
            },
            // We have a new message to send to the clients
            out_opt = send_out_ch.next() => {
                if let None = out_opt {
                    log::warn!("Send out channel closed");
                    std::process::exit(0);
                }
                let msg = out_opt.unwrap();
                for (id, writer) in &writers {
                    if let Err(e) = writer.clone().send(msg.clone()).await {
                        log::info!("Disconnected from client with error: {}", e);
                        to_remove.push(*id);
                    }
                }
            }
        }
        // Remove disconnected clients
        for id in &to_remove {
            writers.remove(id);
        }
        to_remove.clear();
    }
}
