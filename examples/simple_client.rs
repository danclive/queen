#![allow(unused_imports)]
use std::net::{TcpStream, ToSocketAddrs};
use std::io::{self, Write};
use std::io::ErrorKind::{InvalidData, NotConnected};
use std::sync::{Arc, Mutex};
use std::thread;

use queen::nson::{msg, Message};
use queen::dict::*;
use queen::util::message::read_block;
use queen::crypto::{Crypto, Method};
use queen::net::CryptoOptions;

#[derive(Debug, Clone)]
pub struct ClientOptions<A: ToSocketAddrs> {
    pub addr: A,
    pub crypto_options: Option<CryptoOptions>,
    pub auth_message: Option<Message>
}

pub struct Client<A: ToSocketAddrs> {
    inner: Arc<Mutex<Option<ClientInner>>>,
    options: ClientOptions<A>
}

pub struct ClientInner {
    socket: TcpStream,
    crypto: Option<Crypto>
}

impl<A: ToSocketAddrs> Client<A> {
    pub fn new(options: ClientOptions<A>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            options
        }
    }

    pub fn connect(&self) -> io::Result<()> {
        let socket = TcpStream::connect(&self.options.addr)?;

        let mut inner = ClientInner {
            socket,
            crypto: None
        };

        inner.handshake(self.options.crypto_options.clone())?;

        inner.ping()?;

        inner.auth(self.options.auth_message.clone())?;

        *self.inner.lock().unwrap() = Some(inner);

        let inner = self.inner.clone();
        thread::Builder::new().name("client backend".to_string()).spawn(move || {
            Self::backend(inner);
        }).unwrap();

        Ok(())
    }

    pub fn is_connect(&self) -> bool {
        self.inner.lock().unwrap().is_some()
    }

    fn backend(_inner: Arc<Mutex<Option<ClientInner>>>) {
        
    }
}

impl<A: ToSocketAddrs> Drop for Client<A> {
    fn drop(&mut self) {
        *self.inner.lock().unwrap() = None
    }
}

impl ClientInner {
    fn read(&self) -> io::Result<Message> {
        let mut data = read_block(&mut &self.socket)?;

        if let Some(crypto) = &self.crypto {
            let _ = crypto.decrypt(&mut data).map_err(|err|
                io::Error::new(InvalidData, format!("{}", err)
            ));
        }

        let recv = Message::from_slice(&data);

        recv.map_err(|err| io::Error::new(InvalidData, format!("{}", err)))
    }

    fn write(&self, msg: &Message) -> io::Result<()> {
        let mut data = msg.to_vec().expect("InvalidData");

        if let Some(crypto) = &self.crypto {
            let _ = crypto.encrypt(&mut data).map_err(|err|
                io::Error::new(InvalidData, format!("{}", err)
            ));
        }

        (&mut &self.socket).write_all(&data)
    }

    fn handshake(&mut self, options: Option<CryptoOptions>) -> io::Result<()> {
        if let Some(options) = options {
            let hand_msg = msg!{
                HANDSHAKE: options.method.as_str(),
                ACCESS: options.access
            };

            self.write(&hand_msg)?;

            let crypto = Crypto::new(&options.method, options.secret.as_bytes());
            self.crypto = Some(crypto);
        } else {
            let hand_msg = msg!{
                HANDSHAKE: ""
            };

            self.write(&hand_msg)?;
        }

        Ok(())
    }

    fn ping(&mut self) -> io::Result<Message> {
        let ping_msg = msg!{
            CHAN: PING
        };

        self.write(&ping_msg)?;
        let ret = self.read()?;

        Ok(ret)
    }

    fn auth(&mut self, auth_message: Option<Message>) -> io::Result<Message> {
        let mut message = msg!{
            CHAN: AUTH
        };

        if let Some(auth_message) = auth_message {
            message.extend(auth_message);
        }

        self.write(&message)?;
        let ret = self.read()?;

        Ok(ret)
    }
}

fn main() {
    let options = ClientOptions {
        addr: "127.0.0.1:8888",
        crypto_options: None,
        auth_message: None
    };

    let client = Client::new(options);

    client.connect().unwrap();

    println!("{:?}", client.is_connect());
}
