use super::peer::Peer;
use fnv::FnvHashMap as HashMap;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use log;
use std::sync::{mpsc::Sender, Arc, Mutex, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{self, Duration},
};
use tokio_util::codec::{Decoder, Encoder};
use types::{Replica, WireReady};

type Reader = tokio::net::tcp::OwnedReadHalf;
type Writer = tokio::net::tcp::OwnedWriteHalf;
type Err = std::io::Error;

const ID_BYTE_SIZE: usize = std::mem::size_of::<Replica>();

#[derive(Debug)]
enum ConnectionState {
    Connected,
    // TODO maybe pending should include a timeout var??
    Pending((Option<Reader>, Option<Writer>)),
}

pub struct ConnectionManager<I, O, D, E>
where
    I: WireReady + Send + Sync + 'static + Unpin,
    O: WireReady + Clone + Sync + 'static + Unpin,
    D: Decoder<Item = I, Error = Err> + Clone + Send + Sync + 'static,
    E: Encoder<Arc<O>> + Clone + Send + Sync + 'static,
{
    known_peers: RwLock<HashMap<Replica, String>>,
    peers: Mutex<HashMap<Replica, ConnectionState>>,
    id: Replica,
    addr: String,
    tx: Mutex<Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>>,
    dec: D,
    enc: E,
    connecting: Mutex<bool>,
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
        tx: Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
        dec: D,
        enc: E,
    ) -> Arc<Self> {
        let cm = Arc::new(ConnectionManager {
            id,
            addr,
            known_peers: RwLock::new(known_peers),
            tx: Mutex::new(tx),
            // peers: RwLock::new(HashMap::default()),
            peers: Mutex::new(HashMap::default()),
            dec,
            enc,
            connecting: Mutex::new(false),
        });

        {
            let mut peers = cm.peers.lock().unwrap();
            for (id, _) in cm.known_peers.read().unwrap().iter() {
                if *id == cm.id {
                    continue;
                }
                peers.insert(id.clone(), ConnectionState::Pending((None, None)));
            }
        }

        tokio::spawn(Self::listen(cm.clone()));
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        tokio::spawn(Self::connect(cm.clone()));

        cm
    }

    pub fn add_connection(cm: Arc<Self>, id: Replica) {
        let mut peers = cm.peers.lock().unwrap();
        if let ConnectionState::Pending(_) = peers[&id] {
            return;
        }
        peers.insert(id.clone(), ConnectionState::Pending((None, None)));
        if !*cm.connecting.lock().unwrap() {
            tokio::spawn(Self::connect(cm.clone()));
        }
    }

    async fn listen(cm: Arc<Self>) {
        let listener = TcpListener::bind(cm.addr.clone()).await.expect("Failed to listen for connections");

        loop {
            let (conn, from) = listener.accept().await.expect("Listener closed while accepting");
            tokio::spawn(Self::accept_conn(cm.clone(), conn, from));
        }
    }
    
    async fn accept_conn(cm: Arc<Self>, mut conn: TcpStream, from: std::net::SocketAddr) {
        log::info!("New incoming connection from {}", from.clone());
        conn.set_nodelay(true).expect("Failed to set nodelay");

        // TODO Verify the connection is coming from valid node
        let mut id_buf = [0 as u8; ID_BYTE_SIZE];
        conn.read_exact(&mut id_buf).await.expect("Failed to read ID from new connection");
        let id = Replica::from_be_bytes(id_buf);

        if !cm.peers.lock().unwrap().contains_key(&id) {
            cm.peers.lock().unwrap().insert(id, ConnectionState::Pending((None, None)));
        }

        let (read, _) = conn.into_split();

        let mut peers = cm.peers.lock().unwrap();
        if let ConnectionState::Pending((r, _)) = peers.get_mut(&id).unwrap() {
            *r = Some(read);
        }

        // Now check if we have both a reader and a writer
        if let ConnectionState::Pending((Some(_), Some(_))) = peers.get(&id).unwrap() {
            let removed = peers.remove(&id).unwrap();
            if let ConnectionState::Pending((Some(reader), Some(writer))) = removed {//peers.remove(&id).unwrap() {
                let peer = Peer::new(reader, writer, cm.dec.clone(), cm.enc.clone());
                peers.insert(id, ConnectionState::Connected);
                cm.tx.lock().unwrap().send((id, peer.send, peer.recv)).unwrap();
            }
        }
    }

    async fn connect(cm: Arc<Self>) {

        {
            *cm.connecting.lock().unwrap() = true;
        }

        let mut interval = time::interval(Duration::from_millis(100));

        // TODO: Timeout!
        loop {

            interval.tick().await;
            let iter: Vec<usize> = {
                let peers = cm.peers.lock().unwrap();
                peers.iter().filter_map(|value| 
                    match value.1 {
                    ConnectionState::Pending((_, None)) => Some(value.0.clone()),
                    _ => None,
                }).collect()
            };

            if iter.len() <= 0 {
                break;
            }


            for id in iter {

                log::info!("Attempting to connect to {}",id);
                let addr = cm.known_peers.read().unwrap()[&id].clone();

                let mut write = match Self::attempt_conn(addr).await {
                    Err(_) => continue,
                    Ok(w) => w,
                };

                write.write_all(&cm.id.to_be_bytes()).await.expect("Failed to send identification to node");

                let mut peers = cm.peers.lock().unwrap();
                if let ConnectionState::Pending((_, w)) = peers.get_mut(&id).unwrap() {
                    *w = Some(write);
                }

                // Now check if we have both a reader and a writer
                if let ConnectionState::Pending((Some(_), Some(_))) = peers.get(&id).unwrap() {
                    let removed = peers.remove(&id).unwrap();
                    if let ConnectionState::Pending((Some(reader), Some(writer))) = removed {//peers.remove(&id).unwrap() {
                        let peer = Peer::new(reader, writer, cm.dec.clone(), cm.enc.clone());
                        peers.insert(id, ConnectionState::Connected);
                        cm.tx.lock().unwrap().send((id, peer.send, peer.recv)).unwrap();
                    }
                }
            }
        }
        *cm.connecting.lock().unwrap() = false;
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

