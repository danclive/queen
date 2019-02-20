use std::io;
use std::collections::HashMap;
use std::cell::Cell;
use std::i32;
use std::os::unix::io::AsRawFd;

use queen_io::plus::mpms_queue::Queue;
use queen_io::plus::block_queue::BlockQueue;
use nson::msg;

use nson::Value;
use nson::value::Array;

use crate::Message;
use crate::poll::{poll, Ready, Events};

use super::service::Service;

pub struct Control {
    events: Events,
    service: Service,
    session: Session,
    pub queue_i: Queue<(Message)>,
    pub queue_o: BlockQueue<(String, Message)>,
    event_id: Cell<i32>,
    run: bool
}

#[derive(Debug)]
struct Session {
    listens: HashMap<i32, ListenState>,
    links: HashMap<i32, LinkState>,
    node: Vec<i32>,
    chan: HashMap<String, Vec<i32>>,
}

#[derive(Debug)]
struct ListenState {
    protocol: String,
    addr: String
}

#[derive(Debug)]
struct LinkState {
    protocol: String,
    addr: String,
    node: bool,
    handshake: bool,
    my: bool,
    events: HashMap<String, usize>
}

impl Session {
    fn new() -> Session {
        Session {
            listens: HashMap::new(),
            links: HashMap::new(),
            node: Vec::new(),
            chan: HashMap::new(),
        }
    }
}

impl Control {
    pub fn new() -> io::Result<Control> {
        let service = Service::new()?;
        let mut session = Session::new();

        session.links.insert(0, LinkState {
            protocol: "".to_string(),
            addr: "".to_string(),
            node: false,
            handshake: false,
            my: false,
            events: HashMap::new()
        });

        let control = Control {
            events: Events::new(),
            service,
            session,
            queue_i: Queue::with_capacity(16 * 1000)?,
            queue_o: BlockQueue::with_capacity(4 * 1000),
            event_id: Cell::new(0),
            run: true
        };

        Ok(control)
    }

    pub fn run(&mut self) -> io::Result<()> {
        while self.run {
            self.run_once()?;
        }

        Ok(())
    }

    #[inline]
    fn run_once(&mut self) -> io::Result<()> {
        self.events.clear();

        let service_fd = self.service.fd();
        self.events.put(service_fd, Ready::readable());

        let queue_i_fd = self.queue_i.as_raw_fd();
        self.events.put(queue_i_fd, Ready::readable());

        if poll(&mut self.events, None)? > 0 {
            for i in 0..self.events.len() {
                let event = self.events.get(i).unwrap();
                match event.fd() {
                    fd if fd == service_fd => {
                        self.dispatch_service()?;
                    }
                    fd if fd == queue_i_fd => {
                        self.dispatch_queue_i()?;
                    }
                    _ => ()
                }
            }
        }

        Ok(())
    }

    fn dispatch_service(&mut self) -> io::Result<()> {
        loop {
            let message = match self.service.recv() {
                Some(message) => message,
                None => {
                    break;
                }
            };

            macro_rules! queue_o_push {
                ($self:ident, $event:ident, $message:ident) => (
                    if let Some(ids) = $self.session.chan.get($event) {
                        if ids.contains(&0) {
                            $self.queue_o.push(($event.to_string(), $message.clone()));
                        }
                    }
                )
            }

            if let Ok(event) = message.get_str("event") {
                match event {
                    "sys:listen" => {
                        queue_o_push!(self, event, message);
                        self.event_listen(&message);
                    }
                    "sys:unlisten" => {
                        queue_o_push!(self, event, message);
                        self.event_listen(&message);
                    }
                    "sys:link" => {
                        queue_o_push!(self, event, message);
                        self.event_link(&message);
                    }
                    "sys:unlink" => {
                        queue_o_push!(self, event, message);
                        self.event_link(&message);
                    }
                    "sys:accept" => {
                        queue_o_push!(self, event, message);
                        self.event_accept(&message);
                    }
                    "sys:remove" => {
                        queue_o_push!(self, event, message);
                        self.event_remove(&message);
                    }
                    "sys:send" => {
                        queue_o_push!(self, event, message);
                        self.event_send(&message);
                    }
                    "sys:recv" => {
                        queue_o_push!(self, event, message);
                        self.event_recv(&message);
                    }
                    _ => ()
                }
            }
        }

        Ok(())
    }

    fn dispatch_queue_i(&mut self) -> io::Result<()> {
        loop {
            let mut message = match self.queue_i.pop() {
                Some(message) => message,
                None => {
                    break;
                }
            };

            let event = message.get_str("event").expect("Can't get event!").to_string();

            if let Err(_) = message.get_i32("event_id") {
                let event_id = self.get_event_id();
                message.insert("event_id", event_id);
            }

            if event.starts_with("pub:") {
                self.relay_message(0, &event, &message);
            } else if event.starts_with("sys:") {
                match event.as_str() {
                    "sys:hand" => {
                        if let Ok(ok) = message.get_bool("ok") {
                            if let Some(Value::I32(conn_id)) = message.remove("conn_id") {
                                message.remove("protocol");
                                message.remove("addr");

                                if ok {
                                    if let Some(link) = self.session.links.get_mut(&conn_id) {
                                        link.handshake = true;

                                        if let Ok(node) = message.get_bool("node") {
                                            if node {
                                                link.node = true;

                                                self.session.node.push(conn_id);
                                            }
                                        }

                                        let data = message.to_vec().unwrap();
                                        self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                                    }
                                } else {
                                    let data = message.to_vec().unwrap();
                                    self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                                    // send and close ??
                                }
                            }
                        } else {
                            if let Some(Value::I32(conn_id)) = message.remove("conn_id") {
                                let mut message = message;
                                message.insert("node", true);
                                let data = message.to_vec().unwrap();
                                self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                            }
                        }
                    }
                    "sys:handed" => {
                        if let Some(Value::I32(conn_id)) = message.remove("conn_id") {
                            let data = message.to_vec().unwrap();
                            self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                        }
                    }
                    "sys:attach" => {
                        if let Ok(ok) = message.get_bool("ok") {
                            if let Some(Value::I32(conn_id)) = message.remove("conn_id") {
                                macro_rules! attach_reply {
                                    ($message:expr, $ok:expr, $conn_id:expr) => (
                                        let data = $message.to_vec().unwrap();
                                        self.service.send(msg!{"event": "sys:send", "conns": [$conn_id], "data": data}).unwrap();
                                    )
                                }

                                message.remove("protocol");
                                message.remove("addr");

                                let event = message.get_str("v").expect("Can't get v at attach!");
                                if ok {
                                    self.attach(conn_id, &event);
                                    attach_reply!(message, true, conn_id);
                                } else {
                                    attach_reply!(message, false, conn_id);
                                }
                            }
                        } else {
                            let event = message.get_str("v").expect("Can't get v at attach!");
                            self.attach(0, &event);
                        }
                    }
                    "sys:detach" => {
                        self.detach(0, message);
                    }
                    "sys:shoutdown" => {
                        self.run = false;
                    }
                    _ => {
                        self.service.send(message).unwrap();
                    }
                }
            }
        }

        Ok(())
    }

    fn get_event_id(&self) -> i32 {
        let mut id = self.event_id.get() + 1;
        if id >= i32::MAX {
            id = 1;
        }

        self.event_id.replace(id)
    }

    fn event_listen(&mut self, message: &Message) {
        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("listen_id").expect("Can't get listen_id!");
                let protocol = message.get_str("protocol").expect("Can't get protocol!");

                // Todo, unix socket
                if protocol == "tcp" {
                    let addr = message.get_str("addr").expect("Can't get addr!");

                    let listen_state = ListenState {
                        protocol: protocol.to_string(),
                        addr: addr.to_string()
                    };

                    self.session.listens.insert(id, listen_state);
                }
            }
        }
    }

    fn event_link(&mut self, message: &Message) {
        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("conn_id").expect("Can't get conn_id!");
                let protocol = message.get_str("protocol").expect("Can't get protocol!");

                // Todo, unix socket
                if protocol == "tcp" {
                    let addr = message.get_str("addr").expect("Can't get addr!");

                    let link_state = LinkState {
                        protocol: protocol.to_string(),
                        addr: addr.to_string(),
                        node: false,
                        handshake: false,
                        my: true,
                        events: HashMap::new()
                    };

                    self.session.links.insert(id, link_state);
                }
            }
        }
    }

    fn event_accept(&mut self, message: &Message) {
        let id = message.get_i32("conn_id").expect("Can't get conn_id!");
        let protocol = message.get_str("protocol").expect("Can't get protocol!");

        // Todo, unix socket
        if protocol == "tcp" {
            let addr = message.get_str("addr").expect("Can't get addr!");

            let link_state = LinkState {
                protocol: protocol.to_string(),
                addr: addr.to_string(),
                node: false,
                handshake: false,
                my: false,
                events: HashMap::new()
            };

            self.session.links.insert(id, link_state);
        }
    }

    fn event_remove(&mut self, message: &Message) {
        if let Ok(conn_id) = message.get_i32("conn_id") {
            if let Some(link) = self.session.links.remove(&conn_id) {
                for (event, _) in link.events {

                    let mut temps = Vec::new();

                    if let Some(ids) = self.session.chan.get_mut(&event) {
                        if let Some(pos) = ids.iter().position(|x| *x == conn_id) {
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

    fn event_send(&mut self, _message: &Message) {
        //unimplemented!()
    }

    fn event_recv(&mut self, message: &Message) {
        // msg!{
        //  "event_id": 1,
        //  "event": "sys:hand",
        //  "ok": true, // or false
        //  "u": "admin", // username
        //  "p": "admin123" // password
        // };

        let conn_id = message.get_i32("conn_id").expect("Can't get conn_id!");
        let data = message.get_binary("data").expect("Can't get data!");

        let inner_message = match Message::from_slice(&data) {
            Ok(m) => m,
            Err(_) => {
                // Todo return error message
                return
            }
        };

        let inner_event = match inner_message.get_str("event") {
            Ok(event) => event.to_owned(),
            Err(_) => {
                // Todo return error message
                return
            }
        };

        macro_rules! queue_o_push {
            ($self:ident, $event:ident, $message:ident) => (
                if let Some(ids) = $self.session.chan.get(&$event) {
                    if ids.contains(&0) {
                        $self.queue_o.push(($event, $message));
                    }
                }
            )
        }

        if inner_event.starts_with("sys:") {
            match inner_event.as_str() {
                "sys:hand" => {
                    if let Some(Value::Boolean(ok)) = inner_message.get("ok") {
                        if *ok {
                            if let Some(link) = self.session.links.get_mut(&conn_id) {
                                link.handshake = true;

                                if let Ok(node) = inner_message.get_bool("node") {
                                    if node {
                                        link.node = true;

                                        self.session.node.push(conn_id);
                                    }
                                }
                            }
                        }

                        queue_o_push!(self, inner_event, inner_message);
                    } else {
                        if inner_message.get_str("u").is_err() {
                            let mut message = inner_message;
                            message.insert("error", "Can't get u from message!");

                            let data = message.to_vec().unwrap();
                            self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                            return
                        };

                        if inner_message.get_str("p").is_err() {
                            let mut message = inner_message;
                            message.insert("error", "Can't get p from message!");

                            let data = message.to_vec().unwrap();
                            self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                            return
                        };

                        if let Some(link) = self.session.links.get(&conn_id) {
                            let mut message = inner_message;
                            message.insert("conn_id", conn_id);
                            message.insert("protocol", link.protocol.clone());
                            message.insert("addr", link.addr.clone());

                            queue_o_push!(self, inner_event, message);
                        }
                    }
                }
                "sys:handed" => {
                    if let Some(link) = self.session.links.get(&conn_id) {
                        if let Ok(_ok) = message.get_bool("ok") {
                            let mut message = inner_message;
                            message.insert("conn_id", conn_id);

                            queue_o_push!(self, inner_event, message);
                        } else {
                            let mut message = inner_message;

                            if link.handshake {
                                message.insert("ok", true);
                            } else {
                                message.insert("ok", false);
                            }

                            let data = message.to_vec().unwrap();
                            self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                        }
                    }
                },
                "sys:sync" => {

                }
                "sys:attach" => {
                    macro_rules! attach_reply {
                        ($message:ident, $ok:expr, $conn_id:expr) => (
                            let mut message = $message;
                            message.insert("ok", $ok);
                            let data = message.to_vec().unwrap();
                            self.service.send(msg!{"event": "sys:send", "conns": [$conn_id], "data": data}).unwrap();
                        )
                    }

                    if let Ok(event) = inner_message.get_str("v") {
                        if event.starts_with("pub:") {
                            if let Some(ids) = self.session.chan.get(&inner_event) {
                                if ids.contains(&0) {
                                    if let Some(link) = self.session.links.get(&conn_id) {
                                        if link.handshake {
                                            let mut message = inner_message;
                                            message.insert("conn_id", conn_id);
                                            message.insert("protocol", link.protocol.clone());
                                            message.insert("addr", link.addr.clone());

                                            queue_o_push!(self, inner_event, message);
                                        } else {
                                            let mut message = inner_message;
                                            message.insert("error", "Not to handshake!");
                                            attach_reply!(message, false, conn_id);
                                        }
                                    }
                                } else {
                                    self.attach(conn_id, &event);
                                    attach_reply!(inner_message, true, conn_id);
                                }
                            } else {
                                self.attach(conn_id, &event);
                                attach_reply!(inner_message, true, conn_id);
                            }
                        } else {
                            let mut message = inner_message;
                            message.insert("error", "Only attach public event!");
                            attach_reply!(message, false, conn_id);
                        }
                    } else {
                        let mut message = inner_message;
                        message.insert("error", "Message format error: Can't get v from message!");
                        attach_reply!(message, false, conn_id);
                    }
                }
                "sys:detach" => {
                    self.detach(conn_id, inner_message);
                }
                _ => ()
            }
        } else if inner_event.starts_with("pub:") {
            if let Ok(_ok) = inner_message.get_bool("ok") {
                if let Some(ids) = self.session.chan.get("sys:reply") {
                    if ids.contains(&0) {
                        self.queue_o.push(("sys:reply".to_string(), inner_message));
                    }
                }
            } else {
                if let Some(link) = self.session.links.get(&conn_id) {
                    if link.handshake {
                        self.relay_message(conn_id, &inner_event, &inner_message);

                        if let Some(ids) = self.session.chan.get("sys:relay") {
                            if ids.contains(&0) {
                                self.queue_o.push(("sys:relay".to_string(), inner_message));
                            }
                        }
                    } else {
                        let mut message = inner_message;
                        message.insert("ok", false);
                        message.insert("error", "Not to handshake!");
                        let data = message.to_vec().unwrap();
                        self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
                    }
                }
            }
        }
    }

    fn relay_message(&self, conn_id: i32, event: &str, message: &Message) {
        let mut array: Array = Array::new();

        if let Some(conns) = self.session.chan.get(event) {
            for id in conns {
                if id == &0 {
                    self.queue_o.push((event.to_string(), message.clone()));
                } else {
                    if let Some(link) = self.session.links.get(id) {
                        if link.handshake && id != &conn_id {
                            array.push((*id).into());
                        }
                    }
                }
            }
        }

        for id in &self.session.node {
            if conn_id != *id {
                array.push((*id).into());
            }
        }

        if !array.is_empty() {
            let data = message.to_vec().unwrap();
            self.service.send(msg!{"event": "sys:send", "conns": array, "data": data}).unwrap();
        }

        let mut reply = msg!{"event": event, "ok": true};

        if let Ok(event_id) = message.get_i32("event_id") {
            reply.insert("event_id", event_id);
        }

        let data = reply.to_vec().unwrap();
        self.service.send(msg!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
    }

    fn attach(&mut self, conn_id: i32, event: &str) {
        let conns = self.session.chan.entry(event.to_string()).or_insert_with(||Vec::new());
        if !conns.contains(&conn_id) {
            conns.push(conn_id);
        }

        if let Some(link_state) = self.session.links.get_mut(&conn_id) {
            let count = link_state.events.entry(event.to_string()).or_insert(0);
            *count += 1;
        }
    }

    fn detach(&mut self, conn_id: i32, message: Message) {
        macro_rules! detach_reply {
            ($message:ident, $ok:expr, $conn_id:ident) => (
                let mut message = $message;
                message.insert("ok", $ok);
                let data = message.to_vec().unwrap();
                self.service.send(msg!{"event": "sys:send", "conns": [$conn_id], "data": data}).unwrap();
            )
        }

        if let Ok(event) = message.get_str("v") {
            if let Some(link_state) = self.session.links.get_mut(&conn_id) {
                if let Some(count) = link_state.events.get_mut(event) {
                    *count -= 1;

                    if count <= &mut 0 {
                        if let Some(conns) = self.session.chan.get_mut(event) {
                            if let Some(pos) = conns.iter().position(|x| *x == conn_id) {
                                conns.remove(pos);
                            }

                            if conns.is_empty() {
                                self.session.chan.remove(event);
                            }
                        }

                        link_state.events.remove(event);
                    }
                }
            }

            detach_reply!(message, true, conn_id);
        } else {
            let mut message = message;
            message.insert("error", "Message format error: Can't get v from message!");
            detach_reply!(message, false, conn_id);
        }
    }
}
