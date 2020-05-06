#![allow(unused_imports)]
use std::time::Duration;
use std::collections::{HashMap, HashSet};
use std::thread;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering}
};
use std::cell::Cell;
use std::io::ErrorKind::Interrupted;
use std::result;

use queen_io::{
    epoll::{Epoll, Events, Token, Ready, EpollOpt},
    queue::mpsc::Queue
};

use nson::{
    Message, msg,
    message_id::MessageId
};

use slab::Slab;

use rand::{SeedableRng, seq::SliceRandom, rngs::SmallRng};

use crate::stream::Stream;
use crate::dict::*;
use crate::error::{ErrorCode, Result, Error, SendError, RecvError};


struct QueenInner<H> {
    id: MessageId,
    epoll: Epoll,
    events: Events,
    queue: Queue<Packet>,
    rand: SmallRng,
    hook: H,
    run: Arc<AtomicBool>
}

enum Packet {
    NewSocket(Stream<Message>),
    NewClient(Stream<Message>)
}

#[derive(Default)]
pub struct Slot {
    pub sockets: Slab<Socket>,
    // 索引，CHAN，客户端 Token
    pub chans: HashMap<String, HashSet<usize>>,
    // 索引，客户端 ID， 客户端 Token
    pub client_ids: HashMap<MessageId, usize>,
    // 客户端 Token，客户端
    pub clients: HashMap<usize, Client>
}

pub struct Socket {
    pub token: usize, // Socket 会有一个内部 ID，
    pub id: MessageId, // 默认情况下会随机生成，也可以指定
    pub stream: Stream<Message>,
    // Scoket 所关联的客户端，一个或者多个。如果是一个，socket token 和 Client token 相同
    pub clients: SocketClient
}

// socket 通讯的数据包
enum SocketPacket {
    AddClient { token: usize },
    DelClient { token: usize },
    Message { token: usize, message: Message }
}

pub enum SocketClient {
    One(Client),
    More(HashMap<usize, Client>)
}

// 客户端
// 客户端可以绑定到 Slot 或者 Socket 上
pub struct Client {
    pub token: usize, // 客户端会有一个内部 ID
    pub socket: usize, // 客户端所属 socket ID
    pub id: MessageId, // 默认情况下会随机生成一个，可以在认证时修改
    pub label: Message, // 客户端的一些属性，可以在认证时设置
    pub auth: bool, // 是否认证
    pub root: bool, // 是否具有超级权限
    pub chans: HashMap<String, HashSet<String>>, // 客户端 ATTACH 的 CHAN，以及 LABEL
    pub send_messages: Cell<usize>, // 发送的消息数
    pub recv_messages: Cell<usize> // 接收的消息数
}
