use super::peer::Peer;
use fnv::FnvHashMap as HashMap;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::{SinkExt, StreamExt};
use log;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{self, Duration},
    sync::{Notify},
};
use tokio_util::codec::{Decoder, Encoder};
use types::{Replica, WireReady};

type Reader = tokio::net::tcp::OwnedReadHalf;
type Writer = tokio::net::tcp::OwnedWriteHalf;
type Err = std::io::Error;

const ID_BYTE_SIZE: usize = std::mem::size_of::<Replica>();

#[derive(Debug)]
struct Pending {
    reader: Option<Reader>,
    writer: Option<Writer>,
}

#[derive(Debug)]
enum Message {
    Reader(Reader),
    Writer(Writer),
}

#[derive(Debug)]
pub enum Function {
    // AddConnection(Replica, String),
    AddConnection(Replica),
}

pub struct ConnectionManager<I, O, D, E>
where
    I: WireReady + Send + Sync + 'static + Unpin,
    O: WireReady + Clone + Sync + 'static + Unpin,
    D: Decoder<Item = I, Error = Err> + Clone + Send + Sync + 'static,
    E: Encoder<Arc<O>> + Clone + Send + Sync + 'static,
{
    known_peers: HashMap<Replica, String>,
    peers: HashMap<Replica, Pending>,
    id: Replica,
    addr: String,
    completed_connections: UnboundedSender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
    function_calls: UnboundedReceiver<Function>,
    new_connections: UnboundedReceiver<(Replica, Message)>,
    add_connection: UnboundedSender<(Replica, String)>,
    dec: D,
    enc: E,
}

impl<I, O, D, E> ConnectionManager<I, O, D, E>
where
    I: WireReady + Send + Sync + 'static + Unpin,
    O: WireReady + Clone + Sync + 'static + Unpin,
    D: Decoder<Item = I, Error = Err> + Clone + Send + Sync + 'static,
    E: Encoder<Arc<O>> + Clone + Send + Sync + 'static,
{
    pub async fn new(
        id: Replica,
        addr: String,
        known_peers: HashMap<Replica, String>,
        completed_connections: UnboundedSender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
        dec: D,
        enc: E,
    ) -> UnboundedSender<Function> {
        let (new_connections_tx, new_connections) = unbounded::<(Replica, Message)>();
        let (function_caller, function_calls) = unbounded::<Function>();
        let (add_connection, add_connection_rx) = unbounded::<(Replica, String)>();
        let mut cm = ConnectionManager {
            id,
            addr,
            known_peers: known_peers,
            completed_connections,
            function_calls,
            new_connections,
            add_connection,
            peers: HashMap::default(),
            dec,
            enc,
        };

        for (id, _) in cm.known_peers.iter() {
            if *id == cm.id {
                continue;
            }
            cm.peers.insert(id.clone(), Pending{reader: None, writer: None});
        }

        tokio::spawn(Self::event_handler(cm, new_connections_tx, add_connection_rx));
        function_caller
    }

    async fn event_handler(mut cm: Self, new_connections_tx: UnboundedSender<(Replica, Message)>, add_connection_rx: UnboundedReceiver<(Replica, String)>) {

        let notify = Arc::new(Notify::new());
        tokio::spawn(listen(cm.addr, new_connections_tx.clone()));
        tokio::spawn(connect(cm.id, new_connections_tx, add_connection_rx, notify.clone()));
        notify.notify_one();

        for (id, _) in &cm.peers {
            cm.add_connection.send((*id, cm.known_peers.get(id).unwrap().clone())).await.unwrap();
        }

        loop {
            tokio::select! {
                half_connection = cm.new_connections.next() => {
                    let (id, connection) = half_connection.unwrap();
                    match connection {
                        Message::Reader(r) => {
                            if !cm.peers.contains_key(&id) {
                                cm.peers.insert(id, super::connection::Pending{reader: Some(r), writer: None});
                            } else {
                                cm.peers.get_mut(&id).unwrap().reader = Some(r);
                            }
                        },
                        Message::Writer(w) => {
                            if !cm.peers.contains_key(&id) {
                                cm.peers.insert(id, super::connection::Pending{reader: None, writer: Some(w)});
                            } else {
                                cm.peers.get_mut(&id).unwrap().writer = Some(w);
                            }
                        },
                    }
                    // TODO Clean this up..?
                    if let Some(_) = cm.peers.get(&id).unwrap().writer {
                        if let Some(_) = cm.peers.get(&id).unwrap().reader {
                            let peer = cm.peers.remove(&id).unwrap();
                            let p = Peer::new(peer.reader.unwrap(), peer.writer.unwrap(), cm.dec.clone(), cm.enc.clone());
                            cm.completed_connections.send((id, p.send, p.recv)).await.unwrap();
                        }
                    }
                },
                function_call = cm.function_calls.next() => {
                    let function = function_call.unwrap();
                    match function {
                        // Function::AddConnection(id, addr) => {
                        Function::AddConnection(id) => {
                            if !cm.peers.contains_key(&id) {
                                cm.peers.insert(id.clone(), super::connection::Pending{reader: None, writer: None});
                            }
                            cm.add_connection.send((id, cm.known_peers.get(&id).unwrap().clone())).await.unwrap();
                            notify.notify_one();
                        },
                    }
                },
            };
        } 
    }
}

async fn listen(addr: String, new_connections: UnboundedSender<(Replica, Message)>) {
    let listener = TcpListener::bind(addr).await.expect("Failed to listen for connections");

    loop {
        let (conn, from) = listener.accept().await.expect("Listener closed while accepting");
        log::info!("New incoming connection from {}", from);
        tokio::spawn(accept_conn(new_connections.clone(), conn));
    }
}

async fn accept_conn(mut new_connections: UnboundedSender<(Replica, Message)>, mut conn: TcpStream) {
    conn.set_nodelay(true).expect("Failed to set nodelay");

    // TODO Verify the connection is coming from valid node
    let mut id_buf = [0 as u8; ID_BYTE_SIZE];
    conn.read_exact(&mut id_buf).await.expect("Failed to read ID from new connection");
    let id = Replica::from_be_bytes(id_buf);

    let (read, _) = conn.into_split();
    new_connections.send((id, Message::Reader(read))).await.unwrap();
}

async fn connect(my_id: Replica, mut new_connections: UnboundedSender<(Replica, Message)>, mut add_connection: UnboundedReceiver<(Replica, String)>, notify: Arc<Notify>) {

    let mut interval = time::interval(Duration::from_millis(100));

    let mut to_connect = HashMap::default();

    loop {
        tokio::select! {
            _ = notify.notified() => {
                loop {
                    interval.tick().await;

                    // TODO Look at this for iterator?
                    let mut r = add_connection.try_next();
                    while r.is_ok() {
                        let (id, addr) = r.unwrap().unwrap();
                        to_connect.insert(id, addr);
                        r = add_connection.try_next();
                    }

                    let iter: Vec<(Replica, String)> = to_connect.iter().map(|(key, value)| (*key, value.clone())).collect();

                    if iter.len() <= 0 {
                        break;
                    }

                    for (id, addr) in iter {

                        let mut write = match attempt_conn(addr).await {
                            Err(_) => continue,
                            Ok(w) => w,
                        };

                        log::info!("Connected to peer {}", id);
                        write.write_all(&my_id.to_be_bytes()).await.expect("Failed to send identification to node");
                        to_connect.remove(&id);
                        new_connections.send((id, Message::Writer(write))).await.unwrap();
                    }
                }
            },
        }
    }
}

async fn attempt_conn(addr: String) -> Result<Writer, Err> {
    let conn = TcpStream::connect(addr.clone()).await?;
    log::info!("Connected to addr: {}", addr.clone());
    conn.set_nodelay(true)?;
    let (_, write) = conn.into_split();
    Ok(write)
}

