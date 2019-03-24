use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};
use std::thread;

use nson::msg;
use nson::Value;

use log::{trace, debug};

use crate::{Queen, Context};
use crate::poll::{poll, Ready, Events};

use super::service::Service;

#[derive(Clone)]
struct Session {
    inner: Arc<RwLock<SessionInner>>
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
            inner: Arc::new(RwLock::new(SessionInner {
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
        queen.on("sys:listen", move |context| {
            control2.listen(context);
        });

        let control2 = control.clone();
        queen.on("sys:unlisten", move |context| {
            control2.unlisten(context);
        });

        let control2 = control.clone();
        queen.on("sys:link", move |context| {
            control2.link(context);
        });

        let control2 = control.clone();
        queen.on("sys:unlink", move |context| {
            control2.unlink(context);
        });

        let control2 = control.clone();
        queen.on("sys:accept", move |context| {
            control2.accept(context);
        });

        let control2 = control.clone();
        queen.on("sys:remove", move |context| {
            control2.remove(context);
        });

        let control2 = control.clone();
        queen.on("sys:send", move |context| {
            control2.send(context);
        });

        let control2 = control.clone();
        queen.on("sys:recv", move |context| {
            control2.recv(context);
        });

        let control2 = control.clone();
        queen.on("queen:on", move |context| {
            control2.on(context);
        });

        let control2 = control.clone();
        queen.on("queen:off", move |context| {
            control2.off(context);
        });

        let control2 = control.clone();
        queen.on("queen:emit", move |context| {
            control2.emit(context);
        });

        control.run();

        Ok(())
    }

    #[inline]
    pub fn listen(&self, context: Context) {
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

                    let mut session = self.session.inner.write().unwrap();
                    session.listens.insert(id, state);

                    debug!("session.listens: {:?}", session.listens);
                }
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    pub fn unlisten(&self, context: Context) {
        debug!("unlisten: {:?}", context);

        let message = context.message;

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("listen_id")
                    .expect("Can't get listen_id!");

                let mut session = self.session.inner.write().unwrap();
                session.listens.remove(&id);

                debug!("session.listens: {:?}", session.listens);
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    pub fn link(&self, context: Context) {
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

                    let mut session = self.session.inner.write().unwrap();
                    session.links.insert(id, state);

                    debug!("session.links: {:?}", session.links);
                }
            }
        } else {
            self.service.send(message).unwrap();
        }
    }

    #[inline]
    pub fn unlink(&self, context: Context) {
        debug!("unlink: {:?}", context);

        let message = context.message;

        if let Ok(ok) = message.get_bool("ok") {
            if ok {
                let id = message.get_i32("conn_id")
                    .expect("Can't get conn_id!");

                let mut session = self.session.inner.write().unwrap();
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
    pub fn accept(&self, context: Context) {
        debug!("accept: {:?}", context);

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

            let mut session = self.session.inner.write().unwrap();
            session.links.insert(id, state);

            debug!("session.links: {:?}", session.links);
        }
    }

    #[inline]
    pub fn remove(&self, context: Context) {
        debug!("accept: {:?}", context);

        let message = context.message;

        let id = message.get_i32("conn_id")
            .expect("Can't get conn_id!");

        let mut session = self.session.inner.write().unwrap();
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

    #[inline]
    pub fn send(&self, context: Context) {
        debug!("send: {:?}", context);
    }

    #[inline]
    pub fn recv(&self, context: Context) {
        debug!("recv: {:?}", context);
    }

    #[inline]
    pub fn on(&self, context: Context) {
        debug!("on: {:?}", context);
    }

    #[inline]
    pub fn off(&self, context: Context) {
        debug!("off: {:?}", context);
    }

    #[inline]
    pub fn emit(&self, context: Context) {
        let mut message = context.message;

        trace!("emit: {:?}", message);

        let event = match message.get_str("event") {
            Ok(event) => event.to_owned(),
            Err(_) => return
        };

        if let Some(Value::Message(mut message)) = message.remove("message") {
            message.insert("event", event.to_owned());

            if event.starts_with("sys:") {
                context.queen.push(&event, message);
            } else if event.starts_with("pub:") {
                
            }
        }
    }

    pub fn run(self) {
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

    pub fn run_once(&mut self) {
        loop {
            let message = match self.service.recv() {
                Some(message) => message,
                None => break
            };

            let event = match message.get_str("event") {
                Ok(event) => event.to_owned(),
                Err(_) => break
            };

            self.queen.push(&event, message);
        }
    }
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}
