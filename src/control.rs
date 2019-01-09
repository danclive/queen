use std::io;
use std::collections::HashMap;
use std::cell::Cell;
use std::i32;

use queen_io::{Poll, Events, Token, Ready, PollOpt, Event};
use queen_io::plus::mpms_queue::Queue;
use queen_io::plus::block_queue::BlockQueue;
use bsonrs::{doc, bson};

use bsonrs::Value;
use bsonrs::value::Array;

use crate::service::Service;
use crate::Message;

pub struct Control {
	poll: Poll,
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
	const SERVICE: Token = Token(1);
	const QUEUE_I: Token = Token(2);

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
			poll: Poll::new()?,
			events: Events::with_capacity(128),
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
		self.poll.register(&self.service, Self::SERVICE, Ready::readable(), PollOpt::edge())?;
		self.poll.register(&self.queue_i, Self::QUEUE_I, Ready::readable(), PollOpt::edge())?;

		while self.run {
			self.run_once()?;
		}

		Ok(())
	}

	#[inline]
	fn run_once(&mut self) -> io::Result<()> {
		let size = self.poll.wait(&mut self.events, None)?;

		for i in 0..size {
			let event = self.events.get(i).unwrap();
			self.dispatch(event)?;
		}

		Ok(())
	}

	#[inline]
	fn dispatch(&mut self, event: Event) -> io::Result<()> {
		match event.token() {
			Self::SERVICE => {
				self.dispatch_service()?;
			}
			Self::QUEUE_I => {
				self.dispatch_queue_i()?;
			}
			_ => ()
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

			if let Ok(event) = message.get_str("event") {
				//println!("{:?}", event);
				macro_rules! queue_o_push {
					($event:expr) => (
						if let Some(ids) = self.session.chan.get($event) {
							if ids.contains(&0) {
								self.queue_o.push(($event.to_string(), message));
							}
						}
					)
				}

				match event {
					"sys:listen" => {
						self.event_listen(&message);
						queue_o_push!("sys:listen");
					}
					"sys:link" => {
						self.event_link(&message);
						queue_o_push!("sys:link");
					}
					"sys:accept" => {
						self.event_accept(&message);
						queue_o_push!("sys:accept");
					}
					"sys:remove" => {
						self.event_remove(&message);
						queue_o_push!("sys:remove");
					}
					"sys:send" => {
						self.event_send(&message);
						queue_o_push!("sys:send");
					}
					"sys:recv" => {
						self.event_recv(&message);
						queue_o_push!("sys:recv");
					}
					_ => ()
				}
			}
		}

		self.poll.reregister(&self.service, Self::SERVICE, Ready::readable(), PollOpt::edge())?;

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
							if let Some(Value::Int32(conn_id)) = message.remove("conn_id") {

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
										self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
									}
								} else {
									let data = message.to_vec().unwrap();
									self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
								}
							}
						} else {
							if let Some(Value::Int32(conn_id)) = message.remove("conn_id") {
								let data = message.to_vec().unwrap();
								self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
							}
						}
					}
					"sys:attach" => {
						self.attach(0, message);

					}
					"sys:detach" => {
						self.detach(0, message);
					}
					_ => {
						message.insert("event", event);
						self.service.send(message).unwrap();
					}
				}
			}
		}

		self.poll.reregister(&self.queue_i, Self::QUEUE_I, Ready::readable(), PollOpt::edge())?;

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
		// doc!{
		// 	"event_id": 1,
		// 	"event": "sys:hand",
		// 	"ok": true, // or false
		// 	"u": "admin", // username
		// 	"p": "admin123" // password
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

						self.queue_o.push((inner_event, inner_message));
					} else {
						if inner_message.get_str("u").is_err() {
							return
						};

						if inner_message.get_str("p").is_err() {
							return
						};
						let mut inner_message = inner_message;
						inner_message.insert("conn_id", conn_id);

						self.queue_o.push((inner_event, inner_message));
					}
				}
				"sys:handed" => {
					if let Some(link) = self.session.links.get(&conn_id) {
						let mut message = inner_message;

						if link.handshake {
							message.insert("ok", true);
						} else {
							message.insert("ok", false);
						}

						let data = message.to_vec().unwrap();
						self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
					}
				},
				"sys:sync" => {
					//unimplemented!()
				}
				"sys:attach" => {
					self.attach(conn_id, inner_message);
				}
				"sys:detach" => {
					self.detach(conn_id, inner_message);
				}
				_ => ()
			}
		} else if inner_event.starts_with("pub:") {
			if let Ok(_ok) = inner_message.get_bool("ok") {

			} else {
				self.relay_message(conn_id, &inner_event, &inner_message);

				if let Some(ids) = self.session.chan.get("sys:relay") {
					if ids.contains(&0) {
						self.queue_o.push(("sys:relay".to_string(), inner_message));
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
						if link.handshake {
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
			self.service.send(doc!{"event": "sys:send", "conns": array, "data": data}).unwrap();
		}

		let mut reply = doc!{"event": event, "ok": true};

		if let Ok(event_id) = message.get_i32("event_id") {
			reply.insert("event_id", event_id);
		}

		let data = reply.to_vec().unwrap();
		self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
	}

	fn attach(&mut self, conn_id: i32, message: Message) {
		if let Ok(event) = message.get_str("v") {
			let conns = self.session.chan.entry(event.to_string()).or_insert_with(||Vec::new());
			if !conns.contains(&conn_id) {
				conns.push(conn_id);
			}

			if let Some(link_state) = self.session.links.get_mut(&conn_id) {
				let count = link_state.events.entry(event.to_string()).or_insert(0);
				*count += 1;
			}

			let mut message = message;
			message.insert("ok", true);
			let data = message.to_vec().unwrap();
			self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
		} else {
			let mut message = message;
			message.insert("ok", false);
			let data = message.to_vec().unwrap();
			self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
		}
	}

	fn detach(&mut self, conn_id: i32, message: Message) {
		if let Ok(event) = message.get_str("v") {
			if let Some(link_state) = self.session.links.get_mut(&conn_id) {
				let count = link_state.events.entry(event.to_string()).or_insert(0);
				*count -= 1;

				if count <= &mut 0 {
					let mut temp = None;
					if let Some(conns) = self.session.chan.get_mut(event) {
						if let Some(pos) = conns.iter().position(|x| *x == conn_id) {
							conns.remove(pos);
						}

						if conns.is_empty() {
							temp = Some(event.clone());
						}
					}

					if let Some(event) = temp {
						self.session.chan.remove(event);
					}

					link_state.events.remove(event);
		 		}
		 	}

		 	let mut message = message;
			message.insert("ok", true);
			let data = message.to_vec().unwrap();
			self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
	 	} else {
	 		let mut message = message;
			message.insert("ok", false);
			let data = message.to_vec().unwrap();
			self.service.send(doc!{"event": "sys:send", "conns": [conn_id], "data": data}).unwrap();
	 	}
	}
}
