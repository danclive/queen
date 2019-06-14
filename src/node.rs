use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::time::Duration;
use std::rc::Rc;
use std::collections::HashMap;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::borrow::ToOwned;
use std::clone::Clone;

use queen_io::sys::timerfd::{TimerFd, TimerSpec};

use nson::msg;
use nson::{Array, Message};

use log::warn;

use rand;
use rand::seq::SliceRandom;

use crate::poll::{poll, Ready, Events};
use crate::network::Network;

pub struct Session {
    listens: HashMap<i32, ListenState>,
    links: HashMap<i32, LinkState>,
    chan: HashMap<String, Vec<i32>>,
    rand: rand::rngs::ThreadRng
}

#[derive(Debug)]
struct ListenState {
    proto: String,
    addr: String
}

#[derive(Debug)]
struct LinkState {
    proto: String,
    addr: String,
    hand: bool,
    su: bool,
    events: HashMap<String, usize>
}

impl Session {
    fn new() -> Session {
        Session {
            listens: HashMap::new(),
            links: HashMap::new(),
            chan: HashMap::new(),
            rand: rand::thread_rng()
        }
    }

    fn attach(&mut self, conn_id: i32, event: &str) {
        let conns = self.chan.entry(event.to_owned()).or_insert_with(Vec::new);
        if !conns.contains(&conn_id) {
            conns.push(conn_id);
        }

        if let Some(link) = self.links.get_mut(&conn_id) {
            let count = link.events.entry(event.to_owned()).or_insert(0);
            *count += 1;
        }
    }

    fn detach(&mut self, conn_id: i32, event: &str) {
        if let Some(link) = self.links.get_mut(&conn_id) {
            if let Some(count) = link.events.get_mut(event) {
                *count -= 1;

                if *count == 0 {
                    link.events.remove(event);

                    if let Some(conns) = self.chan.get_mut(event) {
                        if let Some(pos) = conns.iter().position(|x| *x == conn_id) {
                            conns.remove(pos);
                        }

                        if conns.is_empty() {
                            self.chan.remove(event);
                        }
                    }
                }
            }
        }
    }
}

pub struct Node {
    pub network: Network,
    pub session: Session,
    pub timer: Timer<Message>,
    pub callback: Callback,
    pub run: bool
}

pub struct Callback {
    pub listen_fn: Option<Rc<dyn Fn(&mut Node, &Message)>>,
    pub unlisten_fn: Option<Rc<dyn Fn(&mut Node, &Message)>>,
    pub unlink_fn: Option<Rc<dyn Fn(&mut Node, &Message)>>,
    pub remove_fn: Option<Rc<dyn Fn(&mut Node, &Message)>>,
    pub accept_fn: Option<Rc<dyn Fn(&mut Node, &Message) -> bool>>,
    pub hand_fn: Option<Rc<dyn Fn(&mut Context) -> bool>>,
    pub attach_fn: Option<Rc<dyn Fn(&mut Context) -> bool>>,
    pub detach_fn: Option<Rc<dyn Fn(&mut Context) -> bool>>,
    pub recv_fn: Option<Rc<dyn Fn(&mut Context) -> bool>>
}

pub struct Context<'a> {
    pub node: &'a mut Node,
    pub id: i32,
    pub message: &'a mut Message
}

impl Node {
    pub fn new() -> io::Result<Self> {
        let node = Node {
            network: Network::new()?,
            session: Session::new(),
            timer: Timer::new()?,
            callback: Callback {
                listen_fn: None,
                unlisten_fn: None,
                accept_fn: None,
                remove_fn: None,
                unlink_fn: None,
                hand_fn: None,
                attach_fn: None,
                detach_fn: None,
                recv_fn: None
            },
            run: true
        };

        Ok(node)
    }

    fn listen(&mut self, message: Message) {
        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("id").expect("Can't get id!");
                let proto = message.get_str("proto").expect("Can't get proto!");

                if proto == "tcp" {
                    let addr = message.get_str("addr").expect("Can't get addr!");

                    let state = ListenState {
                        proto: proto.to_owned(),
                        addr: addr.to_owned()
                    };

                    self.session.listens.insert(id, state);
                }
            }
        }

        if let Some(listen_fn) = &self.callback.listen_fn.clone() {
            listen_fn(self, &message);
        }
    }

    fn unlisten(&mut self, message: Message) {
        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("id").expect("Can't get id!");
                self.session.listens.remove(&id);
            }
        }

        if let Some(unlisten_fn) = &self.callback.unlisten_fn.clone() {
            unlisten_fn(self, &message);
        }
    }

    fn unlink(&mut self, message: Message) {
        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("id").expect("Can't get id!");

                if let Some(link) = self.session.links.remove(&id) {
                    for (event, _) in link.events {
                        let mut temps = Vec::new();

                        if let Some(ids) = self.session.chan.get_mut(&event) {
                            if let Some(pos) = ids.iter().position(|x| *x == id) {
                                ids.remove(pos);
                            }

                            if ids.is_empty() {
                                temps.push(event);
                            }
                        }

                        for temp in temps {
                            self.session.chan.remove(&temp);
                        }
                    }
                }
            }
        }

        if let Some(unlink_fn) = &self.callback.unlink_fn.clone() {
            unlink_fn(self, &message);
        }
    }

    fn remove(&mut self, message: Message) {
        let id = message.get_i32("id").expect("Can't get id!");

        if let Some(link) = self.session.links.remove(&id) {
            for (event, _) in link.events {
                let mut temps = Vec::new();

                if let Some(ids) = self.session.chan.get_mut(&event) {
                    if let Some(pos) = ids.iter().position(|x| *x == id) {
                        ids.remove(pos);
                    }

                    if ids.is_empty() {
                        temps.push(event);
                    }
                }

                for temp in temps {
                    self.session.chan.remove(&temp);
                }
            }
        }

        if let Some(remove_fn) = &self.callback.remove_fn.clone() {
            remove_fn(self, &message);
        }
    }

    fn accept(&mut self, message: Message) {
        let id = message.get_i32("id").expect("Can't get id!");

        let success = if let Some(accept_fn) = &self.callback.accept_fn.clone() {
            accept_fn(self, &message)
        } else {
            true
        };

        if success {
            let proto = message.get_str("proto").expect("Can't get proto!");

            if proto == "tcp" {
                let addr = message.get_str("addr").expect("Can't get addr!");

                let state = LinkState {
                    proto: proto.to_owned(),
                    addr: addr.to_owned(),
                    hand: false,
                    su: false,
                    events: HashMap::new()
                };

                self.session.links.insert(id, state);
            }
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        let mut events = Events::new();

        let service_fd = self.network.event_fd();
        events.put(service_fd, Ready::readable());

        let timer_fd = self.timer.as_raw_fd();
        events.put(timer_fd, Ready::readable());

        while self.run {
            if poll(&mut events, None).unwrap() > 0 {
                for event in &events {
                    match event.fd() {
                        fd if fd == service_fd => {
                            if event.readiness().is_readable() {
                                self.handle_service()?;
                            }
                        }
                        fd if fd == timer_fd => {
                            if event.readiness().is_readable() {
                                self.handle_timer()?;
                            }
                        }
                        _ => ()
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn send(&mut self, message: Message) -> io::Result<()> {
        if let Ok(time) = message.get_u32("_time") {
            if time > 0 {
                let task = Task {
                    data: message,
                    time: Duration::from_millis(u64::from(time)),
                    id: None
                };

                self.timer.push(task)?;

                return Ok(())
            }
        }

        let event = message.get_str("event").expect("Can't get event!");

        self.relay_su(event, message.clone())?;

        self.network.send(message);

        Ok(())
    }

    #[inline]
    fn handle_service(&mut self) -> io::Result<()> {
        while let Some(message)  = self.network.recv() {
            self.route(message)?;
        }

        Ok(())
    }

    #[inline]
    fn route(&mut self, message: Message) -> io::Result<()> {
        let event = message.get_str("event").expect("Can't get event!");

        self.relay_su(event, message.clone())?;

        if let Ok(conn_id) = message.get_i32("origin_id") {
            let mut message = message.clone();
            message.remove("origin_id");

            let msg = msg!{
                "event": "node:node",
                "msg": message,
                "ok": true
            };

            self.send(msg!{
                "event": "net:send",
                "conns": [conn_id],
                "data": msg.to_vec().unwrap()
            })?;
        }

        match event {
            "net:recv" => self.recv(message)?,
            "net:accept" => self.accept(message),
            "net:remove" => self.remove(message),
            "net:listen" => self.listen(message),
            "net:unlisten" => self.unlisten(message),
            "net:unlink" => self.unlink(message),
            _ => ()
        }

        Ok(())
    }

    fn recv(&mut self, message: Message) -> io::Result<()> {
        let conn_id = message.get_i32("id").expect("Can't get conn_id!");
        let data = message.get_binary("data").expect("Can't get data!");

        let mut message = match Message::from_slice(&data) {
            Ok(msg) => msg,
            Err(err) => {
                warn!("sys:recv, message decode error, id: {:?}, err: {:?}", conn_id, err);
                return Ok(())
            }
        };

        let success = if let Some(recv_fn) = self.callback.recv_fn.clone() {
            let mut context = Context {
                node: self,
                id: conn_id,
                message: &mut message
            };

            recv_fn(&mut context)
        } else {
            true
        };

        if success {
            let event = message.get_str("event").map(ToOwned::to_owned).expect("Can't get event!");

            if event.starts_with("node:") {
                match event.as_str() {
                    "node:hand" => self.hand(conn_id, message)?,
                    "node:attach" => self.attach(conn_id, message)?,
                    "node:detach" => self.detach(conn_id, message)?,
                    "node:node" => self.node(conn_id, message)?,
                    "node:deltime" => self.deltime(conn_id, message)?,
                    _ => {
                        warn!("event unsupport: {:?} conn_id: {:?}, message: {:?}", event, conn_id, message);

                        let mut message = message;
                        message.insert("ok", false);
                        message.insert("error", "Event unsupport!");

                        self.send(msg!{
                            "event": "net:send",
                            "conns": [conn_id],
                            "data": message.to_vec().unwrap()
                        })?;
                    }
                }

            } else if event.starts_with("pub:") {
                // reply client if client provide _id
                if let Some(id) = message.get("_id") {
                    let reply_msg = msg!{
                        "_id": id.clone(),
                        "ok": true
                    };

                    self.send(msg!{
                        "event": "net:send",
                        "conns": [conn_id],
                        "data": reply_msg.to_vec().unwrap()
                    })?;
                }

                if let Ok(time) = message.get_u32("_time") {
                    if time > 0 {

                        let mut id = None;

                        if let Ok(timeid) = message.get_str("_timeid") {
                            id = Some(timeid.to_owned());
                        }

                        let task = Task {
                            data: message,
                            time: Duration::from_millis(u64::from(time)),
                            id
                        };

                        self.timer.push(task)?;

                        return Ok(())
                    }
                }

                self.relay(&event, message)?;
            } else {
                warn!("event prefix unsupport: {:?} conn_id: {:?}, message: {:?}", event, conn_id, message);


                let mut message = message;
                message.insert("ok", false);
                message.insert("error", "Event prefix unsupport!");

                self.send(msg!{
                    "event": "net:send",
                    "conns": [conn_id],
                    "data": message.to_vec().unwrap()
                })?;
            }
        }

        Ok(())
    }

    fn hand(&mut self, conn_id: i32, message: Message) -> io::Result<()> {
        if !message.contains_key("ok") {
            let mut message = message;

            let success = if let Some(hand_fn) = self.callback.hand_fn.clone() {
                let mut context = Context {
                    node: self,
                    id: conn_id,
                    message: &mut message
                };

                hand_fn(&mut context)
            } else {
                true
            };

            if success {
                if let Some(link) = self.session.links.get_mut(&conn_id) {
                    link.hand = true;

                    if let Ok(su) = message.get_bool("su") {
                        if su {
                            link.su = true;
                        }
                    }

                    message.insert("ok", true);

                    self.send(msg!{
                        "event": "net:send",
                        "conns": [conn_id],
                        "data": message.to_vec().unwrap()
                    })?;
                }
            }
        }

        Ok(())
    }

    fn attach(&mut self, conn_id: i32, message: Message) -> io::Result<()> {
        if !message.contains_key("ok") {
            let mut message = message;

            if let Ok(event) = message.get_str("value").map(ToOwned::to_owned ) {
                let access = {
                    if let Some(link) = self.session.links.get(&conn_id) {
                        if link.hand {
                            // true
                            if event.starts_with("node:") || event.starts_with("net") {
                                link.su
                            } else {
                                true
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if access {
                    let success = if let Some(attach_fn) = self.callback.attach_fn.clone() {
                        let mut context = Context {
                            node: self,
                            id: conn_id,
                            message: &mut message
                        };

                        attach_fn(&mut context)
                    } else {
                        true
                    };

                    if success {
                        self.session.attach(conn_id, &event);

                        message.insert("ok", true);
                    } else {
                        message.insert("ok", false);
                        message.insert("error", "Not auth!");
                    }
                } else {
                    message.insert("ok", false);
                    message.insert("error", "No permission!");
                }
            } else {
                message.insert("ok", false);
                message.insert("error", "Can't get v from message!");
            }

            self.send(msg!{
                "event": "net:send",
                "conns": [conn_id],
                "data": message.to_vec().unwrap()
            })?;
        }

        Ok(())
    }

    fn detach(&mut self, conn_id: i32, message: Message) -> io::Result<()> {
        if !message.contains_key("ok") {
            let mut message = message;

            if let Ok(event) = message.get_str("value").map(ToOwned::to_owned ) {
                let success = if let Some(detach_fn) = self.callback.detach_fn.clone() {
                    let mut context = Context {
                        node: self,
                        id: conn_id,
                        message: &mut message
                    };

                    detach_fn(&mut context)
                } else {
                    true
                };

                if success {
                    self.session.detach(conn_id, &event);
                }

                message.insert("ok", true);
            } else {
                message.insert("ok", false);
                message.insert("error", "Can't get v from message!");
            }

            self.send(msg!{
                "event": "net:send",
                "conns": [conn_id],
                "data": message.to_vec().unwrap()
            })?;
        }

        Ok(())
    }

    fn relay(&mut self, event: &str, message: Message) -> io::Result<()> {
        let mut array: Vec<i32> = Vec::new();

        if let Some(conns) = self.session.chan.get(event) {
            for id in conns {
                if let Some(link) = self.session.links.get(id) {
                    if link.hand {
                        array.push(*id);
                    }
                }
            }
        }

        if array.is_empty() {
            return Ok(())
        }

        // if has share and share == true
        if let Ok(share) = message.get_bool("_share") {
            if share {
                if array.len() == 1 {
                    self.send(msg!{
                        "event": "net:send",
                        "conns": array,
                        "data": message.to_vec().unwrap()
                    })?;
                } else {
                    if let Some(id) = array.choose(&mut self.session.rand) {
                        self.send(msg!{
                            "event": "net:send",
                            "conns": [*id],
                            "data": message.to_vec().unwrap()
                        })?;
                    }
                }

                return Ok(())
            }
        }

        self.send(msg!{
            "event": "net:send",
            "conns": array,
            "data": message.to_vec().unwrap()
        })?;

        Ok(())
    }

    fn relay_su(&mut self, event: &str, message: Message) -> io::Result<()> {
        let mut array: Array = Array::new();

        if let Some(conns) = self.session.chan.get(event) {
            for id in conns {
                if let Some(link) = self.session.links.get(id) {
                    if link.su {
                        array.push((*id).into());
                    }
                }
            }
        }

        if !array.is_empty() {
            self.send(msg!{
                "event": "net:send",
                "conns": array,
                "data": message.to_vec().unwrap()
            })?;
        }

        Ok(())
    }

    fn node(&mut self, conn_id: i32, message: Message) -> io::Result<()> {
        let mut message = message;

        if let Some(link) = self.session.links.get(&conn_id) {
            if link.su {
                if let Ok(mut m) = message.get_message("msg").map(Clone::clone) {
                    if let Ok(e) = m.get_str("event") {
                        if e == "net:listen" || e == "net:unlisten" || e == "net:link" || e == "net:unlink" || e == "net:send" {
                            m.insert("origin_id", conn_id);

                            self.send(m)?;

                            return Ok(());
                        } else {
                            message.insert("ok", false);
                            message.insert("error", "Event unsupport!");
                        }
                    } else {
                        message.insert("ok", false);
                        message.insert("error", "Can't get e from m!");
                    }
                } else {
                    message.insert("ok", false);
                    message.insert("error", "Can't get m from message!");
                }
            } else {
                message.insert("ok", false);
                message.insert("error", "No permission!");
            }

            self.send(msg!{
                "event": "net:send",
                "conns": [conn_id],
                "data": message.to_vec().unwrap()
            })?;
        }

        Ok(())
    }

    fn deltime(&mut self, conn_id: i32, message: Message) -> io::Result<()> {
        let mut message = message;
        
        if let Ok(timeid) = message.get_str("_timeid") {
            self.timer.remove(timeid.to_owned());
            message.insert("ok", true);
        } else {
            message.insert("ok", false);
            message.insert("error", "Can't get _timeid from message!");
        }

        self.send(msg!{
            "event": "net:send",
            "conns": [conn_id],
            "data": message.to_vec().unwrap()
        })?;

        Ok(())
    }

    #[inline]
    fn handle_timer(&mut self) -> io::Result<()> {
        self.timer.done()?;

        if let Some(task) = self.timer.pop() {
            if let Ok(event) = task.data.get_str("event").map(ToOwned::to_owned) {
                if event.starts_with("net:") {
                    self.relay_su(&event, task.data.clone())?;
                    self.network.send(task.data);
                } else if event.starts_with("pub:") {
                    self.relay(&event, task.data)?;
                }
            }
        }

        if let Some(task) = self.timer.peek() {
            self.timer.settime(true, TimerSpec {
                interval: Duration::new(0, 0),
                value: task.time
            })?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Task<T> {
    pub data: T,
    pub time: Duration,
    pub id: Option<String>
}

impl<T> PartialOrd for Task<T> {
    fn partial_cmp(&self, other: &Task<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Task<T> {
    fn cmp(&self, other: &Task<T>) -> Ordering {
        match self.time.cmp(&other.time) {
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater
        }
    }
}

impl<T> PartialEq for Task<T> {
    fn eq(&self, other: &Task<T>) -> bool {
        self.time == other.time
    }
}

impl<T> Eq for Task<T> {}

pub struct Timer<T> {
    tasks: BinaryHeap<Task<T>>,
    timerfd: TimerFd
}

impl<T: Clone> Timer<T> {
    pub fn new() -> io::Result<Timer<T>> {
        Ok(Timer {
            tasks: BinaryHeap::new(),
            timerfd: TimerFd::new()?
        })
    }

    pub fn peek(&self) -> Option<&Task<T>> {
        self.tasks.peek()
    }

    pub fn push(&mut self, task: Task<T>) -> io::Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let mut task = task;
        task.time += now;

        if let Some(peek_task) = self.peek() {
            if task > *peek_task {
                self.settime(true, TimerSpec {
                    interval: Duration::new(0, 0),
                    value: task.time
                })?;
            }
        } else {
            self.settime(true, TimerSpec {
                interval: Duration::new(0, 0),
                value: task.time
            })?;
        }

        self.tasks.push(task);

        Ok(())
    }

    #[inline]
    pub fn pop(&mut self) -> Option<Task<T>> {
        self.tasks.pop()
    }

    #[inline]
    pub fn settime(&self, abstime: bool, value: TimerSpec) -> io::Result<TimerSpec>{
        self.timerfd.settime(abstime, value)
    }

    #[inline]
    pub fn gettime(&self) -> io::Result<TimerSpec> {
        self.timerfd.gettime()
    }

    #[inline]
    pub fn done(&self) -> io::Result<u64> {
        self.timerfd.read()
    }

    pub fn remove(&mut self, id: String) {
        let id = Some(id);

        let mut tasks_vec: Vec<Task<T>> = Vec::from(self.tasks.clone());

        if let Some(pos) = tasks_vec.iter().position(|x| x.id == id) {
            tasks_vec.remove(pos);
        }

        self.tasks = tasks_vec.into();
    }
}

impl<T> AsRawFd for Timer<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.timerfd.as_raw_fd()
    }
}
