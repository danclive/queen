use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{
    Arc,
    mpsc::{Sender}
};

use crate::dict::*;
use crate::nson::{msg, Message, MessageId};
use crate::crypto::Crypto;
use crate::error::{Result};
use crate::util::{
    oneshot::{Sender as OneshotSender}
};

use super::stream::Stream;
use super::worker::Handle;

pub struct Session {
    pub stream: Option<Stream>,
    pub sending: HashMap<MessageId, OneshotSender<Result<()>>>,
    pub attaching: HashMap<u32, OneshotSender<Result<()>>>,
    pub calling: HashMap<MessageId, OneshotSender<Result<Message>>>,
    pub chans: HashMap<String, HashSet<String>>, // HashMap<Chan, HashSet<Label>>
    pub chans2: HashMap<String, HashSet<u32>>, // HashMap<Chan, id>
    pub recvs: HashMap<u32, (String, Sender<Message>, HashSet<String>)>, // HashMap<id, (Chan, tx, Labels)>
    pub recvs2: HashMap<u32, (String, HashSet<String>)>, // HashMap<id, (Chan, tx, Labels)>
    pub handles: HashMap<u32, Arc<Box<Handle>>>
}

impl fmt::Debug for Session {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Session")
            .field("stream", &self.stream)
            .field("sending", &self.sending)
            .field("attaching", &self.attaching)
            .field("calling", &self.calling)
            .field("chans", &self.chans)
            .field("chans2", &self.chans2)
            .field("recvs", &self.recvs)
            .field("recvs2", &self.recvs2)
            .finish()
    }
}

impl Session {
    pub fn new() -> Session {
        Session {
            stream: None,
            sending: HashMap::new(),
            attaching: HashMap::new(),
            calling: HashMap::new(),
            chans: HashMap::new(),
            chans2: HashMap::new(),
            recvs: HashMap::new(),
            recvs2: HashMap::new(),
            handles: HashMap::new()
        }
    }

    pub fn is_connect(&self) -> bool {
        self.stream.is_some()
    }

    pub fn attach(
        &mut self,
        crypto: &Option<Crypto>,
        id: u32,
        chan: &str,
        labels_set: &mut HashSet<String>,
        attaching_tx: OneshotSender<Result<()>>
    ) -> Result<()> {
        let must_attach = !self.chans.contains_key(chan);

        // chan
        let set = self.chans.entry(chan.to_string()).or_insert_with(HashSet::new);

        if must_attach {
            // attach
            let mut message = msg!{
                CHAN: ATTACH,
                ATTACH_ID: id,
                VALUE: chan
            };

            if !labels_set.is_empty() {
                let labels: Vec<String> = labels_set.iter().map(|s| s.to_string()).collect();
                message.insert(LABEL, labels);
            }

            if let Some(stream) = &self.stream {
                let data = Crypto::encrypt_message(crypto, &message)?;
                stream.write(&data)?;
            }

            // attaching
            self.attaching.insert(id, attaching_tx);
        } else if !labels_set.is_empty() {
            let diff_labels = &*labels_set - set;

            if !diff_labels.is_empty() {
                let mut message = msg!{
                    CHAN: ATTACH,
                    ATTACH_ID: id,
                    VALUE: chan
                };

                let labels: Vec<String> = diff_labels.iter().map(|s| s.to_string()).collect();
                message.insert(LABEL, labels);

                if let Some(stream) = &self.stream {
                    let data = Crypto::encrypt_message(crypto, &message)?;
                    stream.write(&data)?;
                }

                // attaching
                self.attaching.insert(id, attaching_tx);
            } else {
                attaching_tx.send(Ok(()));
            }
        } else {
            attaching_tx.send(Ok(()));
        }

        set.extend(labels_set.clone());

        Ok(())
    }

    pub fn detach(
        &mut self,
        crypto: &Option<Crypto>,
        id: u32,
        chan: String,
        labels: HashSet<String>
    ) -> Result<()> {
        let mut remove_chan = false;

        if let Some(ids) = self.chans2.get_mut(&chan) {
            ids.remove(&id);

            if ids.is_empty() {
                remove_chan = true;
            } else if chan != UNKNOWN {
                let mut labels_set = HashSet::new();

                for value in self.recvs.values() {
                    labels_set.extend(value.2.clone());
                }

                for value in self.recvs2.values() {
                    labels_set.extend(value.1.clone());
                }

                let diff_labels = &labels - &labels_set;

                if !diff_labels.is_empty() {

                    let change: Vec<String> = diff_labels.iter().map(|s| s.to_string()).collect();

                    let message = msg!{
                        CHAN: DETACH,
                        VALUE: &chan,
                        LABEL: change
                    };

                    if let Some(stream) = &self.stream {
                        let data = Crypto::encrypt_message(crypto, &message)?;
                        stream.write(&data)?;
                    }
                    // chans 1
                    self.chans.insert(chan.to_string(), labels_set);
                }
            }
        }

        if remove_chan {
            self.chans.remove(&chan);
            self.chans2.remove(&chan);

            if chan != UNKNOWN {
                let message = msg!{
                    CHAN: DETACH,
                    VALUE: chan
                };

                if let Some(stream) = &self.stream {
                    let data = Crypto::encrypt_message(crypto, &message)?;
                    stream.write(&data)?;
                }
            }
        }

        Ok(())
    }

    pub fn detach_no_send(
        &mut self,
        id: u32,
        chan: String,
        labels: HashSet<String>
    ) {
        let mut remove_chan = false;

        if let Some(ids) = self.chans2.get_mut(&chan) {
            ids.remove(&id);

            if ids.is_empty() {
                remove_chan = true;
            } else {
                let mut labels_set = HashSet::new();

                for value in self.recvs.values() {
                    labels_set.extend(value.2.clone());
                }

                for value in self.recvs2.values() {
                    labels_set.extend(value.1.clone());
                }

                let diff_labels = &labels - &labels_set;

                if !diff_labels.is_empty() {
                    // chans 1
                    self.chans.insert(chan.to_string(), labels_set);
                }
            }
        }

        if remove_chan {
            self.chans.remove(&chan);
            self.chans2.remove(&chan);
        }
    }
}
