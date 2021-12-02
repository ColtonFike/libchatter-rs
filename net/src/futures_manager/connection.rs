use super::peer::Peer;
use fnv::FnvHashMap as HashMap;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use log;
use std::sync::{Arc, Mutex, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{self, Duration},
    sync::{mpsc::{self, Sender, Receiver}},
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
enum ConnectionState {
    Connected,
    Pending(Pending),
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
    tx: std::sync::mpsc::Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
    r: Receiver<(Replica, Message)>,
    t: Sender<(Replica, Message)>,
    fun: Receiver<Function>,
    dec: D,
    enc: E,
    connect: Arc<Mutex<bool>>,
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
        tx: std::sync::mpsc::Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
        dec: D,
        enc: E,
    ) -> Sender<Function> {
    // ){
        let (t, r) = mpsc::channel(100);
        let (send, fun) = mpsc::channel(100);
        let mut cm = ConnectionManager {
            id,
            addr,
            known_peers: known_peers,
            tx: tx,
            r,
            t,
            fun,
            peers: HashMap::default(),
            dec,
            enc,
            connect: Arc::new(Mutex::new(false)),
        };

        {
            for (id, _) in cm.known_peers.iter() {
                if *id == cm.id {
                    continue;
                }
                cm.peers.insert(id.clone(), Pending{reader: None, writer: None});
            }
        }

        tokio::spawn(Self::event_handler(cm));
        send
    }


    pub async fn event_handler(mut cm: Self) {

        let (mut t, mut r) = mpsc::channel(100);
        tokio::spawn(Self::listen(cm.addr, cm.t.clone()));
        tokio::spawn(Self::connect(cm.id, cm.t.clone(), r, cm.connect.clone()));

        for (id, _) in &cm.peers {
            t.send((*id, cm.known_peers.get(id).unwrap().clone())).await.unwrap();
        }
        loop {
            tokio::select! {
                mm = cm.r.recv() => {
                    let m = mm.unwrap();
                    match m.1 {
                        // TODO Handle unwrap errors for when peer doesn't exist
                        Message::Reader(r) => cm.peers.get_mut(&m.0).unwrap().reader = Some(r),
                        Message::Writer(w) => cm.peers.get_mut(&m.0).unwrap().writer = Some(w),
                    }
                    if let Some(_) = cm.peers.get(&m.0).unwrap().writer {
                        if let Some(_) = cm.peers.get(&m.0).unwrap().reader {
                            let peer = cm.peers.remove(&m.0).unwrap();
                            let p = Peer::new(peer.reader.unwrap(), peer.writer.unwrap(), cm.dec.clone(), cm.enc.clone());
                            cm.tx.send((m.0, p.send, p.recv)).unwrap();
                        }
                    }
                },
                mm = cm.fun.recv() => {
                    let fun = mm.unwrap();
                    match fun {
                        // Function::AddConnection(id, addr) => {
                        Function::AddConnection(id) => {
                            log::info!("Attempting to reconnect to {}", id.clone());
                            log::info!("Connections! {}", *cm.connect.lock().unwrap());
                            cm.peers.insert(id.clone(), super::connection::Pending{reader: None, writer: None});
                            if !*cm.connect.lock().unwrap() {
                                let (a,b) = mpsc::channel(100);
                                t = a;
                                r = b;
                                tokio::spawn(Self::connect(cm.id, cm.t.clone(), r, cm.connect.clone()));
                            }
                            t.send((id, cm.known_peers.get(&id).unwrap().clone())).await.unwrap();
                        },
                    }
                },
            };
        } 
    }

    async fn listen(addr: String, tx: Sender<(Replica, Message)>) {
        let listener = TcpListener::bind(addr).await.expect("Failed to listen for connections");
        let mut count = 0;

        loop {
            let (conn, from) = listener.accept().await.expect("Listener closed while accepting");
            tokio::spawn(Self::accept_conn(tx.clone(), conn, from));
            count += 1;
            if count >= 2 {
                println!("Closed node listener!");
                return;
            }
        }
    }

    async fn accept_conn(tx: Sender<(Replica, Message)>, mut conn: TcpStream, from: std::net::SocketAddr) {
        log::info!("New incoming connection from {}", from.clone());
        conn.set_nodelay(true).expect("Failed to set nodelay");

        // TODO Verify the connection is coming from valid node
        let mut id_buf = [0 as u8; ID_BYTE_SIZE];
        conn.read_exact(&mut id_buf).await.expect("Failed to read ID from new connection");
        let id = Replica::from_be_bytes(id_buf);
        log::info!("ID of new connection {}", id.clone());

        let (read, _) = conn.into_split();
        tx.send((id, Message::Reader(read))).await.unwrap();
    }

    // TODO Maintain bool to signal when this loop is running
    async fn connect(my_id: Replica, tx: Sender<(Replica, Message)>, mut rx: Receiver<(Replica, String)>, connect: Arc<Mutex<bool>>) {

        let mut interval = time::interval(Duration::from_millis(100));

        let mut to_connect = HashMap::default();
        *connect.lock().unwrap() = true;
        log::info!("Starting Connections! {}", *connect.lock().unwrap());
        loop {

            interval.tick().await;

            let mut r = rx.try_recv();
            while r.is_ok() {
                let (id, addr) = r.unwrap();
                to_connect.insert(id, addr);
                r = rx.try_recv();
            }

            let iter: Vec<(Replica, String)> = to_connect.iter().map(|(key, value)| (*key, value.clone())).collect();

            if iter.len() <= 0 {
                break;
            }


            for (id, addr) in iter {

                // log::info!("Attempting to connect to {}", id);

                let mut write = match Self::attempt_conn(addr).await {
                    Err(_) => continue,
                    Ok(w) => w,
                };

                write.write_all(&my_id.to_be_bytes()).await.expect("Failed to send identification to node");
                to_connect.remove(&id);
                tx.send((id, Message::Writer(write))).await.unwrap();
            }
        }
        *connect.lock().unwrap() = false;
        log::info!("Ending Connections! {}", *connect.lock().unwrap());
    }

    async fn attempt_conn(addr: String) -> Result<Writer, Err> {
        let conn = TcpStream::connect(addr.clone()).await?;
        log::info!("Connected to addr: {}", addr.clone());
        conn.set_nodelay(true)?;
        log::info!("NoDelay set to addr: {}", addr);
        let (_, write) = conn.into_split();
        Ok(write)
    }
}

