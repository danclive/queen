use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::thread;

use nson::msg;
use nson::{Value, Array, Message};

use log::{trace, debug, warn};

use crate::{Queen, Context};
use crate::poll::{poll, Ready, Events};

use super::service::Service;

#[derive(Clone)]
struct Session {
    inner: Arc<Mutex<SessionInner>>
}

struct SessionInner {
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
            inner: Arc::new(Mutex::new(SessionInner {
                listens: HashMap::new(),
                links: HashMap::new(),
                node: Vec::new(),
                chan: HashMap::new(),
            }))
        }
    }

    // fn bbb(&self) {
    //     //println!("{:?}", self.a);
    //     let session = self.inner.lock().unwrap();
    //     println!("{:?}", session.a);
    // }

    // fn ccc(&self) {
    //     //self.a = 123;
    //     //println!("{:?}", self.a);
    //     let mut session = self.inner.lock().unwrap();
    //     session.a = 123;
    //     println!("{:?}", session.a);
    // }
}

#[derive(Clone)]
pub struct Control {
    queen: Queen,
    service: Service,
    session: Session
}

impl Control {
    pub fn bind(queen: &Queen) -> io::Result<()> {
        is_send::<Control>();
        is_sync::<Control>();

        let control = Control {
            queen: queen.clone(),
            service: Service::new()?,
            session: Session::new()
        };

        let control2 = control.clone();
        queen.on("s:listen", move |context| {
            control2.listen(context);
        });

        let control2 = control.clone();
        queen.on("s:unlisten", move |context| {
            control2.unlisten(context);
        });

        let control2 = control.clone();
        queen.on("s:link", move |context| {
            control2.link(context);
        });

        let control2 = control.clone();
        queen.on("s:unlink", move |context| {
            control2.unlink(context);
        });

        let control2 = control.clone();
        queen.on("s:accept", move |context| {
            control2.accept(context);
        });

        let control2 = control.clone();
        queen.on("s:remove", move |context| {
            control2.remove(context);
        });

        let control2 = control.clone();
        queen.on("s:send", move |context| {
            control2.send(context);
        });

        let control2 = control.clone();
        queen.on("s:recv", move |context| {
            control2.recv(context);
        });

        let control2 = control.clone();
        queen.on("q:on", move |context| {
            control2.on(context);
        });

        let control2 = control.clone();
        queen.on("q:off", move |context| {
            control2.off(context);
        });

        let control2 = control.clone();
        queen.on("q:emit", move |context| {
            control2.emit(context);
        });

        control.run();

        Ok(())
    }

    #[inline]
    fn listen(&self, context: Context) {
        debug!("listen: {:?}", context);

        let message = context.message;

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("listen_id")
                    .expect("Can't get listen_id!");
                let protocol = message.get_str("protocol")
                    .expect("Can't get protocol!");

                if protocol == "tcp" {
                    let addr = message.get_str("addr")
                        .expect("Can't get addr!");

                    let state = ListenState {
                        protocol: protocol.to_owned(),
                        addr: addr.to_owned()
                    };

                    let mut session = self.session.inner.lock().unwrap();
                    session.listens.insert(id, state);

                    debug!("session.listens: {:?}", session.listens);
                }
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    fn unlisten(&self, context: Context) {
        debug!("unlisten: {:?}", context);

        let message = context.message;

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("listen_id")
                    .expect("Can't get listen_id!");

                let mut session = self.session.inner.lock().unwrap();
                session.listens.remove(&id);

                debug!("session.listens: {:?}", session.listens);
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    fn link(&self, context: Context) {
        debug!("link: {:?}", context);

        let message = context.message;

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("conn_id")
                    .expect("Can't get conn_id!");
                let protocol = message.get_str("protocol")
                    .expect("Can't get protocol!");

                if protocol == "tcp" {
                    let addr = message.get_str("addr")
                        .expect("Can't get addr!");

                    let state = LinkState {
                        protocol: protocol.to_owned(),
                        addr: addr.to_owned(),
                        node: false,
                        handshake: false,
                        my: true,
                        events: HashMap::new()
                    };

                    let mut session = self.session.inner.lock().unwrap();
                    session.links.insert(id, state);

                    debug!("session.links: {:?}", session.links);
                }
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    fn unlink(&self, context: Context) {
        debug!("unlink: {:?}", context);

        let message = context.message;

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("conn_id")
                    .expect("Can't get conn_id!");

                let mut session = self.session.inner.lock().unwrap();
                if let Some(link) = session.links.remove(&id) {
                    for (event, _) in link.events {
                        let mut temps = Vec::new();

                        if let Some(ids) = session.chan.get_mut(&event) {
                            if let Some(pos) = ids.iter().position(|x| *x == id) {
                                ids.remove(pos);
                            }

                            if ids.is_empty() {
                                temps.push(event);
                            }
                        }

                        for temp in temps {
                            session.chan.remove(&temp);
                        }
                    }
                }
                debug!("session.links: {:?}", session.links);
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    fn accept(&self, context: Context) {
        let message = context.message;

        let id = message.get_i32("conn_id")
            .expect("Can't get conn_id!");
        let protocol = message.get_str("protocol")
            .expect("Can't get protocol!");

        if protocol == "tcp" {
            let addr = message.get_str("addr")
                .expect("Can't get addr!");

            let state = LinkState {
                protocol: protocol.to_owned(),
                addr: addr.to_owned(),
                node: false,
                handshake: false,
                my: false,
                events: HashMap::new()
            };

            let mut session = self.session.inner.lock().unwrap();
            session.links.insert(id, state);
            debug!("session.links: {:?}", session.links);
        }
    }

    #[inline]
    fn remove(&self, context: Context) {
        debug!("remove: {:?}", context);

        let message = context.message;

        let id = message.get_i32("conn_id")
            .expect("Can't get conn_id!");

        let mut session = self.session.inner.lock().unwrap();
        if let Some(link) = session.links.remove(&id) {
            for (event, _) in link.events {
                let mut temps = Vec::new();

                if let Some(ids) = session.chan.get_mut(&event) {
                    if let Some(pos) = ids.iter().position(|x| *x == id) {
                        ids.remove(pos);
                    }

                    if ids.is_empty() {
                        temps.push(event);
                    }
                }

                for temp in temps {
                    session.chan.remove(&temp);
                }
            }
        }
        // debug!("session.links: {:?}", session.links);
    }

    #[inline]
    fn send(&self, context: Context) {
        if !context.message.contains_key("ok") {
            let _ = self.service.send(context.message);
        }
    }

    #[inline]
    fn recv(&self, context: Context) {
        // debug!("recv: {:?}", context);
        // println!("{:?}", "recv");
        let message = context.message;

        let conn_id = message.get_i32("conn_id")
            .expect("Can't get conn_id!");
        let data = message.get_binary("data")
            .expect("Can't get data!");

        let message = match Message::from_slice(&data) {
            Ok(msg) => msg,
            Err(err) => {
                warn!("sys:recv, message decode error, conn_id: {:?}, err: {:?}", conn_id, err);
                return
            }
        };

        let event = match message.get_str("e") {
            Ok(event) => event.to_owned(),
            Err(_) => {
                warn!("sys:recv, can't get event, conn_id:{:?}, message: {:?}", conn_id, message);
                return
            }
        };

        if event.starts_with("s:") {
            match event.as_str() {
                "s:h" => self.hand_from_recv(conn_id, message),
                "s:a" => self.attach_from_recv(conn_id, message),
                "s:d" => self.detach(conn_id, message),
                _ => warn!("event unsupport: {:?}, conn_id: {:?}, message: {:?}", event, conn_id, message)
            }
        } else if event.starts_with("p:") {
            self.relay(conn_id, &event, message.clone());
            context.queen.push(&event, message);
        } else {
            warn!("event prefix is unsupport: {:?} conn_id: {:?}, message: {:?}", event, conn_id, message);
        }
    }

    #[inline]
    fn on(&self, _context: Context) {
        // debug!("on: {:?}", context);
    }

    #[inline]
    fn off(&self, context: Context) {
        debug!("off: {:?}", context);
    }

    #[inline]
    fn emit(&self, context: Context) {
        let mut message = context.message;

        trace!("emit: {:?}", message);

        let event = match message.get_str("e") {
            Ok(event) => event.to_owned(),
            Err(_) => return
        };

        if let Some(Value::Message(mut message)) = message.remove("m") {
            message.insert("e", event.to_owned());

            if event.starts_with("s:") {
                match event.as_str() {
                    "s:h" => self.hand_from_emit(message),
                    "s:a" => self.attach_from_emit(message),
                    _ => context.queen.push(&event, message)
                }

            } else if event.starts_with("p:") {
                self.relay(0, &event, message.clone());
                context.queen.push(&event, message);
            }
        }
    }

    #[inline]
    fn hand_from_recv(&self, conn_id: i32, message: Message) {
        if let Ok(_ok) = message.get_bool("ok") {

        } else {
            let session = self.session.inner.lock().unwrap();
            if let Some(link) = session.links.get(&conn_id) {
                let mut message = message;
                message.insert("conn_id", conn_id);
                message.insert("protocol", link.protocol.clone());
                message.insert("addr", link.addr.clone());

                self.queen.push("s:h", message);
            }
        }
    }

    #[inline]
    fn hand_from_emit(&self, message: Message) {
        if let Ok(ok) = message.get_bool("ok") {
            let mut message = message;
            if let Some(Value::I32(conn_id)) = message.remove("conn_id") {

                message.remove("protocol");
                message.remove("addr");

                if ok {
                    let mut session = self.session.inner.lock().unwrap();
                    if let Some(link) = session.links.get_mut(&conn_id) {
                        link.handshake = true;

                        if let Ok(node) = message.get_bool("node") {
                            if node {
                                link.node = true;
                                if !session.node.contains(&conn_id) {
                                    session.node.push(conn_id);
                                }
                            }
                        }

                        self.queen.emit("s:send", msg!{
                            "e": "s:send",
                            "conns": [conn_id],
                            "data": message.to_vec().unwrap()
                        });
                    }
                }
            }
        } else {

        }
    }

    fn attach_from_recv(&self, conn_id: i32, message: Message) {
        if !message.contains_key("ok") {
            let mut message = message;

            if let Ok(event) = message.get_str("v") {
                if event.starts_with("p:") {
                    let mut session = self.session.inner.lock().unwrap();
                    if let Some(link) = session.links.get_mut(&conn_id) {
                        if !link.handshake {
                            message.insert("ok", false);
                            message.insert("error", "Not handshake!");
                        } else {
                            message.insert("conn_id", conn_id);
                            message.insert("protocol", link.protocol.clone());
                            message.insert("addr", link.addr.clone());
                            message.insert("node", link.node);

                            self.queen.push("s:a", message);
                            return
                        }
                    }
                } else {
                    message.insert("ok", false);
                    message.insert("error", "Only attach public event!");
                }
            } else {
                message.insert("ok", false);
                message.insert("error", "Can't get v from message!");
            }

            self.queen.emit("s:send", msg!{
                "e": "s:send",
                "conns": [conn_id],
                "data": message.to_vec().unwrap()
            });
        }
    }

    fn attach_from_emit(&self, message: Message) {
        if let Ok(ok) = message.get_bool("ok") {
            let mut message = message;
            if let Some(Value::I32(conn_id)) = message.remove("conn_id") {
                message.remove("protocol");
                message.remove("addr");
                message.remove("node");

                let event = message.get_str("v")
                    .expect("Can't get v at attach!");

                if ok {
                    self.attach(conn_id, event);
                }

                self.queen.emit("s:send", msg!{
                    "e": "s:send",
                    "conns": [conn_id],
                    "data": message.to_vec().unwrap()
                });
            }
        }
    }

    fn attach(&self, conn_id: i32, event: &str) {
        let mut session = self.session.inner.lock().unwrap();
        let conns = session.chan.entry(event.to_owned()).or_insert_with(||Vec::new());
        if !conns.contains(&conn_id) {
            conns.push(conn_id);
        }

        if let Some(link) = session.links.get_mut(&conn_id) {
            let count = link.events.entry(event.to_owned()).or_insert(0);
            *count += 1;
        }
    }

    fn detach(&self, conn_id: i32, message: Message) {
        let mut message = message;

        if let Ok(event) = message.get_str("v") {
            let mut session = self.session.inner.lock().unwrap();
            if let Some(link) = session.links.get_mut(&conn_id) {
                if let Some(count) = link.events.get_mut(event) {
                    *count -= 1;

                    if *count <= 0 {
                        link.events.remove(event);

                        if let Some(conns) = session.chan.get_mut(event) {
                            if let Some(pos) = conns.iter().position(|x| *x == conn_id) {
                                conns.remove(pos);
                            }

                            if conns.is_empty() {
                                session.chan.remove(event);
                            }
                        }
                    }
                }

                message.insert("ok", true);
            }
        } else {
            message.insert("ok", false);
            message.insert("error", "Can't get v from message!");
        }

        self.queen.emit("s:send", msg!{
            "e": "s:send",
            "conns": [conn_id],
            "data": message.to_vec().unwrap()
        });
    }

    fn relay(&self, conn_id: i32, event: &str, message: Message) {
        let mut array: Array = Array::new();

        let session = self.session.inner.lock().unwrap();
        if let Some(conns) = session.chan.get(event) {
            for id in conns {
                if let Some(link) = session.links.get(id) {
                    if link.handshake && id != &conn_id {
                        array.push((*id).into());
                    }
                }
            }
        }

        if !array.is_empty() {
            self.queen.push("s:send", msg!{
                "e": "s:send",
                "conns": array,
                "data": message.to_vec().unwrap()
            });
        }

        if conn_id != 0 {
            let mut message = message;
            message.insert("ok", true);

            self.queen.push("s:send", msg!{
                "e": "s:send",
                "conns": [conn_id],
                "data": message.to_vec().unwrap()
            });
        }
    }

    fn run(self) {
        thread::Builder::new().name("control".into()).spawn(move || {
            let mut control = self;

            let mut events = Events::new();
            events.put(control.service.fd(), Ready::readable());

            loop {
                if poll(&mut events, None).unwrap() > 0 {
                    control.run_once();
                }
            }
        }).unwrap();
    }

    fn run_once(&mut self) {
        loop {
            let message = match self.service.recv() {
                Some(message) => message,
                None => break
            };

            let event = match message.get_str("e") {
                Ok(event) => event.to_owned(),
                Err(_) => break
            };

            self.queen.push(&event, message);
        }
    }
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}
