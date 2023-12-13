use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SendError, TryRecvError};
use std::time::{Duration, Instant};
use std::{str, thread};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use slog::Logger;
use actix_web::Responder;
use futures::channel::oneshot;

use protobuf::Message as PbMessage;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use slog::o;
use slog::{Drain, Level};
use std::io::{self};

use actix_web::{web, App, HttpResponse, HttpServer};

// const NODE_LIST: [&'static str; 3] = ["127.0.0.1:9000","127.0.0.1:9001", "127.0.0.1:9002"];
const NODE_LIST: [&'static str; 3] = ["172.31.35.207:9000","172.31.24.122:9000", "172.31.2.67:9000"];

enum Prop {
    Data(Vec<u8>,oneshot::Sender<Option<()>>,),
    Conf(ConfChange)
}

use std::net::UdpSocket;
use std::io::{ErrorKind};

fn raft_listen(addr: &str, sender: Sender<Message>) -> io::Result<()> {
    let socket = UdpSocket::bind(addr)?;
    let mut buffer = [0; 65507]; // Maximum UDP packet size

    loop {
        match socket.recv_from(&mut buffer) {
            Ok((size, _src)) => {
                let message = Message::parse_from_bytes(&buffer[..size]).unwrap();
                sender.send(message).unwrap();
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                // Handle the case where there's no data to be read
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

fn send_messages_thread(msg_receiver: Receiver<Vec<Message>>) {
    let timeout = Duration::from_millis(10);
    let socket = UdpSocket::bind("0.0.0.0:0").expect("couldn't bind");
    loop {
        match msg_receiver.recv_timeout(timeout) {
            Ok(msgs) => {
                for msg in msgs {
                    let node_id = msg.to;
                    let addr = get_addr(node_id as usize);
                    match msg.write_to_bytes() {
                        Ok(data) => {
                            if let Err(e) = socket.send_to(&data, addr) {
                                eprintln!("Failed to send message to {}: {}", addr, e);
                            }
                        },
                        Err(e) => eprintln!("Failed to serialize message: {}", e),
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn get_addr(id: usize) -> &'static str {
    NODE_LIST[id-1]
}

fn get_config() -> Config {
    Config {
        max_size_per_msg: 1024 * 1024 * 1024,
        max_inflight_msgs: 1024,
        applied: 0,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

fn handle_committed_entries(node: &mut RawNode<MemStorage>,
                            store: &MemStorage,
                            committed_entries: Vec<Entry>,
                            response_map: &mut HashMap<u64, oneshot::Sender<Option<()>>>
) {
    for entry in committed_entries {
        let last_apply_index = entry.index;
        if entry.data.is_empty() {
            continue;
        }
        if let EntryType::EntryConfChange = entry.get_entry_type() {
            let mut cc = ConfChange::default();
            cc.merge_from_bytes(&entry.data).unwrap();
            let cs = node.apply_conf_change(&cc).unwrap();
            store.wl().set_conf_state(cs);
        } else {
            // HANDLE NORMAL ENTRY
        }
        if node.raft.state == StateRole::Leader {
            if let Some(sender) = response_map.remove(&last_apply_index) {
                let _ = sender.send(Some(()));
            }
        }
    }
}

fn on_ready(node: &mut RawNode<MemStorage>,
            network_send: Sender<Vec<Message>>,
            response_map: &mut HashMap<u64, oneshot::Sender<Option<()>>>) {
    if !node.has_ready() {
        return;
    }
    let mut ready = node.ready();

    if !ready.messages().is_empty() {
        network_send.send( ready.take_messages()).unwrap();
    }

    let store = node.raft.raft_log.store.clone();
    if !ready.snapshot().is_empty() {
        store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
    }

    handle_committed_entries(node, &store, ready.take_committed_entries(), response_map);

    if !ready.entries().is_empty() {
        store.wl().append(ready.entries()).unwrap();
    }

    // Hard state
    if let Some(hs) = ready.hs() {
        store.wl().set_hardstate(hs.clone());
    }

    if !ready.persisted_messages().is_empty() {
        network_send.send(ready.take_persisted_messages()).unwrap();
    }

    let mut light_rd = node.advance(ready);
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    network_send.send(light_rd.take_messages()).unwrap();
    handle_committed_entries(node,  &store, light_rd.take_committed_entries(), response_map);
    node.advance_apply();
}

fn create_follower(node_id: u64, logger: &Logger) -> RawNode<MemStorage> {
    let mut cfg = get_config();
    cfg.id = node_id;
    let logger = logger.new(o!("tag" => format!("peer_{}", node_id)));
    let storage = MemStorage::new();
    RawNode::new(&cfg, storage, &logger).unwrap()
}

fn initialize_all_followers(proposer_channel: Sender<Prop>) {
    for i in 1..NODE_LIST.len()+1 {
        let mut conf_change = ConfChange::default();
        conf_change.node_id = i as u64;
        conf_change.set_change_type(ConfChangeType::AddNode);
        proposer_channel.send(Prop::Conf(conf_change)).unwrap();
        thread::sleep(Duration::from_millis(100));
    }
}

fn initialize_leader(logger: &Logger) -> RawNode<MemStorage> {
    let mut cfg = get_config();
    cfg.id = 1;
    let logger = logger.new(o!("tag" => format!("peer_{}", 1)));
    let mut s = Snapshot::default();
    s.mut_metadata().index = 1;
    s.mut_metadata().term = 1;
    s.mut_metadata().mut_conf_state().voters = vec![1];
    let storage = MemStorage::new();
    storage.wl().apply_snapshot(s).unwrap();
    RawNode::new(&cfg, storage, &logger).unwrap()
}

fn run_node(proposals: Receiver<Prop>,
            raft_channel: Receiver<Message>,
            network_send: Sender<Vec<Message>>,
            mut node: RawNode<MemStorage>) {
    let mut t = Instant::now();
    let timeout = Duration::from_millis(10);
    let mut response_hashmap: HashMap<u64, oneshot::Sender<Option<()>>> = HashMap::default();

    loop {
        loop {
            match raft_channel.recv_timeout(timeout) {
                Ok(msg) => {
                    node.step(msg).unwrap();
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
        if t.elapsed() >= Duration::from_millis(100) {
            // println!("====> TICK: {:?}",std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
            node.tick();
            t = Instant::now();
        }

        // Only listen to proposals if we become a leader
        if node.raft.state == StateRole::Leader {
            match proposals.try_recv() {
                Ok(msg) => {
                    match msg {
                        Prop::Data(data, roneshot) => {
                            let idx = node.raft.raft_log.last_index() + 1;
                            node.propose(vec![], data).unwrap();
                            if node.raft.raft_log.last_index() + 1 > idx {
                                response_hashmap.insert(idx,roneshot);
                            }
                        },
                        Prop::Conf(conf) => node.propose_conf_change(vec![], conf).unwrap()
                    }
                },
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => return,
            }
        }

        on_ready(&mut node, network_send.clone(), &mut response_hashmap);
    }
}

async fn handle_post_request(body: web::Bytes, proposals: web::Data<Sender<Prop>>) -> impl Responder {
    let data = body.to_vec();
    let (tx, rx) = oneshot::channel();

    proposals.send(Prop::Data(data,tx)).unwrap();
    match rx.await {
        Ok(message) => match message {
            None => HttpResponse::InternalServerError().body("Error processing request"),
            Some(_) => HttpResponse::Ok().body("OK"),
        },
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("Error receiving one shot {:?}", e))
        }
    }
}

fn start_web_server(proposals: Sender<Prop>) {
    let proposals = web::Data::new(proposals);

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.spawn(async move {
        let server = HttpServer::new(move || {
            App::new()
                .app_data(proposals.clone())
                .service(web::resource("/post").route(web::post().to(handle_post_request)))
        })
            .bind("0.0.0.0:8080")
            .unwrap()
            .run();

        let _ = server.await;
    });
}

fn main() {

    // Logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain)
        .chan_size(4096)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    let drain = drain.filter_level(Level::Info).fuse();
    let logger = slog::Logger::root(drain, o!());

    let node_id = u64::from_str(&env::var("NODE_ID").unwrap()).unwrap();

    let (prop_s, prop_r) = mpsc::channel();
    let (channel_s, channel_r) = mpsc::channel();
    let (network_send, network_recv) = mpsc::channel();
    
    let listener = thread::spawn(move || raft_listen(get_addr(node_id as usize), channel_s).unwrap());
    let sender = thread::spawn(move || send_messages_thread(network_recv));

    let node = if node_id==1 {
        initialize_leader(&logger)
    } else {
        create_follower(node_id, &logger)
    };

    let raft_node_runner = thread::spawn(move ||
        run_node(prop_r,channel_r, network_send, node)
    );


    if node_id==1 {
        initialize_all_followers(prop_s.clone());
        thread::sleep(Duration::from_millis(5000));
        start_web_server(prop_s);
    }


    raft_node_runner.join().unwrap();
    listener.join().unwrap();
    sender.join().unwrap();
}