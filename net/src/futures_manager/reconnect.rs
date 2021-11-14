use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use std::sync::{ Arc, mpsc::Sender};
use tokio::{
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
    reconnections: HashSet<Replica>,
    node_addr: HashMap<Replica, String>,
    my_id: Replica,
    dec: D,
    enc: E,
    tx: Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
}

impl<I, O, D, E> ReconnectionManager<I, O, D, E>
where 
I:WireReady + Send + Sync + 'static + Unpin,
O:WireReady + Clone + Sync + 'static + Unpin, 
D:Decoder<Item=I, Error=Err> + Clone + Send + Sync + 'static,
E:Encoder<Arc<O>> + Clone + Send + Sync + 'static,
{
    pub fn new(
        node_addr: HashMap<Replica, String>,
        my_id: Replica,
        dec: D,
        enc: E,
        tx: Sender<(Replica, UnboundedSender<Arc<O>>, UnboundedReceiver<I>)>,
    ) -> Self {
        ReconnectionManager {
            reconnections: HashSet::default(),
            node_addr,
            enc,
            dec,
            my_id,
            tx,
        }
    }

    pub fn add_new_reconnection(&mut self, reconn_id: Replica, timeout: u64) {
        if self.reconnections.contains(&reconn_id) {
            log::warn!("Already reconnecting to peer {}", reconn_id);
            return;
        }

        self.reconnections.insert(reconn_id);
        log::info!("Detected new reconnection!");

        tokio::spawn(Self::reconnect(
            self.my_id, 
            self.node_addr[&self.my_id].clone(),
            reconn_id, 
            self.node_addr[&reconn_id].clone(), 
            self.tx.clone(), 
            self.dec.clone(), 
            self.enc.clone(), 
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
        timeout: u64) 
    {
        let reader = tokio::spawn(Self::start_listener(my_addr)).await.expect("Failed to connect to disconnected node");
        tokio::time::sleep(std::time::Duration::from_secs(40)).await;
        let writer = Self::start_conn(my_id, reconn_addr).await;
        log::info!("Successfullly reconnected!");

        let peer = Peer::new(reader, writer, dec, enc);
        tx.send((reconn_id, peer.send, peer.recv)).unwrap();
    }

    async fn start_listener(addr: String) -> Reader {
        let listener = TcpListener::bind(addr).await.expect("Failed to listen for dropped peer");
        log::info!("Listener Opened!");
        let (mut conn, _) = listener.accept().await.expect("Failed to listen to incoming connection");
        conn.set_nodelay(true).expect("Failed to set nodelay");
        let mut id_buf = [0 as u8; ID_BYTE_SIZE];
        conn.read_exact(&mut id_buf).await.expect("Failed to read ID bytes");
        let _ = Replica::from_be_bytes(id_buf);
        let (read, _) = conn.into_split();
        read
    }


    async fn start_conn(my_id: Replica, addr: String) -> Writer {
        let id_buf =  my_id.to_be_bytes();
        let conn = TcpStream::connect(addr).await.expect("Failed to connect to a disconnected node");
        log::info!("Attempting To Connect!");
        conn.set_nodelay(true).expect("Failed to enable nodelay");
        let (_, mut writer) = conn.into_split();
        writer.write_all(&id_buf).await.expect("Failed to send identification to node");
        writer
    }
}
