use std::thread;
use std::time::Duration;
use std::fmt;
use std::sync::{
    Arc,
    atomic::{Ordering}
};

use crate::dict::*;
use crate::nson::{Message, MessageId};
use crate::crypto::Crypto;
use crate::util::{
    block_queue::BlockQueue
};

use super::Client;

pub struct Worker {
    pub client: Client
}

pub type WorkerQueue = BlockQueue<WorkerQueueMessage>;

pub type Handle = dyn Fn(Message) -> Message + Sync + Send + 'static;

#[derive(Clone)]
pub struct WorkerQueueMessage {
    pub from_id: MessageId,
    pub req_id: MessageId,
    pub req_message: Message,
    pub handle: Arc<Box<Handle>>
}

impl fmt::Debug for WorkerQueueMessage {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("WorkerQueueMessage")
            .field("from_id", &self.from_id)
            .field("req_id", &self.req_id)
            .field("req_message", &self.req_message)
            .finish()
    }
}

impl Worker {
    pub fn run(self, name: String) {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        thread::Builder::new().name(name.clone()).spawn(move || {

            while self.client.inner.run.load(Ordering::Relaxed) {
                if let Some(WorkerQueueMessage { from_id, req_id, req_message, handle })
                    = self.client.inner.worker_queue.pop_timeout(Duration::from_secs(5)) 
                {
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        let mut res_message = handle(req_message);

                        res_message.insert(TO, from_id);
                        res_message.insert(REQUEST_ID, req_id);
                        res_message.insert(CHAN, RECV);

                        if let Ok(data) = Crypto::encrypt_message(&self.client.inner.crypto, &res_message) {
                            let session = self.client.inner.session.lock().unwrap();
                            if !session.is_connect() {
                                return
                            }

                            if let Err(err) = session.stream.as_ref().unwrap().write(&data) {
                                log::error!("write(&data): {:?}", err);
                            }
                        }
                    }));
                }
            }

            log::trace!("{:?} exit", name);
        }).unwrap();
    }
}
