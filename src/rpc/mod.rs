use std::sync::Arc;
use std::collections::HashMap;
use std::io;
use std::thread;

use nson;

use client::Client;

pub type Handle = Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static;

#[derive(Clone)]
pub struct RPC {
	pub client: Client,
	pub handles: HashMap<String, Arc<Box<Handle>>>
}

impl RPC {
	pub fn new(addr: &str) -> io::Result<RPC> {
		Ok(RPC {
			client: Client::new(addr)?,
			handles: HashMap::new()
		})
	}

	pub fn register<H>(&mut self, method: &str, handle: H)
		where H: Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static
	{
		self.handles.insert(method.to_owned(), Arc::new(Box::new(handle)));
	}

	pub fn run(&self, worker_num: usize, username: &str, password: &str) -> io::Result<()> {
		let methods: Vec<String> = self.handles.keys().map(|k| k.to_owned()).collect();

		self.client.connect(username, password, methods)?;

		let mut threads = Vec::new();

		for _ in 0..worker_num {

			let client = self.client.clone();
			//let handles = self.handles.clone();

			threads.push(thread::spawn(move || {
				// client.watch(move |message| {
				// 	unimplemented!()
				// })
				client.run().unwrap();
			}));
		}

		for thread in threads {
            let _ = thread.join().unwrap();
        }

		Ok(())
	}
}