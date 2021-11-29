use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use std::sync::{ Arc, Mutex, mpsc::Sender};
use tokio::{
    time::{Duration, self, Instant},
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use types::{Replica, WireReady};
use log;
use tokio_util::codec::{
    Decoder, 
    Encoder, 
};
use super::peer::Peer;

type Reader = tokio::net::tcp::OwnedReadHalf;
type Writer = tokio::net::tcp::OwnedWriteHalf;
type Err = std::io::Error;

const ID_BYTE_SIZE:usize = std::mem::size_of::<Replica>();

pub struct ReconnectionManager<I, O, D, E>
where 
I:WireReady + Send + Sync + 'static + Unpin,
O:WireReady + Clone + Sync + 'static + Unpin, 
D:Decoder<Item=I, Error=Err> + Clone + Send + Sync + 'static,
E:Encoder<Arc<O>> + Clone + Send + Sync + 'static,
{
    reconnections: Arc<Mutex<HashMap<Replica, (Option<Writer>, Option<Reader>)>>>,
    node_addrs: HashMap<Replica, String>,
    id: Replica,
    dec: D,
    enc: E,
    tx: Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
    listener: TcpListener,
}

impl<I, O, D, E> ReconnectionManager<I, O, D, E>
where 
I:WireReady + Send + Sync + 'static + Unpin,
O:WireReady + Clone + Sync + 'static + Unpin, 
D:Decoder<Item=I, Error=Err> + Clone + Send + Sync + 'static,
E:Encoder<Arc<O>> + Clone + Send + Sync + 'static,
{
    pub async fn new(
        node_addrs: HashMap<Replica, String>,
        id: Replica,
        dec: D,
        enc: E,
        tx: Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
    ) -> Self {
        let listener = TcpListener::bind(node_addrs[&id].clone()).await.expect("Failed to listen for dropped peer");
        ReconnectionManager {
            reconnections: Arc::new(Mutex::new(HashMap::default())),
            node_addrs,
            enc,
            dec,
            id,
            tx,
            listener,    
        }
    }

    // TODO crashes on two disconnections at a time because we try to open two listeners
    pub fn add_new_reconnection(&mut self, reconn_id: Replica, timeout: u64) {
        // do not start attempt at reconnecting if we are already attempting to connect
        if self.reconnections.lock().unwrap().contains_key(&reconn_id) {
            return;
        }

        self.reconnections.lock().unwrap().insert(reconn_id, (None, None));
        log::info!("Attempting to reconnect to peer {}", reconn_id);

        // make reconnect check the listener for connection, on connection, connect to them
        tokio::spawn(Self::reconnect(
            self.id, 
            self.node_addrs[&self.id].clone(),
            reconn_id, 
            self.node_addrs[&reconn_id].clone(), 
            self.tx.clone(), 
            self.dec.clone(), 
            self.enc.clone(), 
            self.reconnections.clone(),
            timeout
        ));
    }
    
    async fn reconnect(
        my_id: Replica, 
        my_addr: String,
        reconn_id: Replica, 
        reconn_addr: String, 
        tx: Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>, 
        dec: D,
        enc: E,
        remove_from: Arc<Mutex<HashMap<Replica, (Option<Writer>, Option<Reader>)>>>,
        timeout: u64) 
    {
        let time_to_timeout = Instant::now().checked_add(Duration::from_secs(timeout)).unwrap();
        let writer;

        let mut interval = time::interval(Duration::from_millis(100));
        // try to connect to disconnected node
        loop {
            // wait 100ms before checking for back online
            interval.tick().await;

            // successful connection!
            if let Ok(w) = Self::attempt_conn(my_id, &reconn_addr).await {
                writer = w;
                break; 
            } else if Instant::now() > time_to_timeout {
                log::info!("Reconnection timed out");
                return;
            }
        }
        // start_listener, timeout if no connection in time
        if let Ok(reader) = time::timeout_at(time_to_timeout, Self::start_listener(my_addr)).await {
            // create peer and send the data protocol manager
            let peer = Peer::new(reader, writer, dec, enc);
            tx.send((reconn_id, peer.send, peer.recv)).unwrap();
        } else {
            log::info!("Reconnection timed out");
        }
        // regardless of success or timeout, remove peer from reconnections list
        remove_from.lock().unwrap().remove(&reconn_id);
    }

    async fn start_listener(addr: String) -> Reader {
        let listener = TcpListener::bind(addr).await.expect("Failed to listen for dropped peer");
        let (mut conn, _) = listener.accept().await.expect("Failed to listen to incoming connection");
        conn.set_nodelay(true).expect("Failed to set nodelay");
        let mut id_buf = [0 as u8; ID_BYTE_SIZE];
        conn.read_exact(&mut id_buf).await.expect("Failed to read ID bytes");
        let _ = Replica::from_be_bytes(id_buf);
        let (read, _) = conn.into_split();
        read
    }


    async fn attempt_conn(my_id: Replica, addr: &str) -> Result<Writer,Err> {
        let conn = TcpStream::connect(addr).await?;
        conn.set_nodelay(true).expect("Failed to enable nodelay");
        let (_, mut writer) = conn.into_split();
        writer.write_all(&my_id.to_be_bytes()).await.expect("Failed to send identification to node");
        Ok(writer)
    }
}
