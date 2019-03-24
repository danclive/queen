use std::net::{TcpListener, TcpStream};
use std::io;
use std::thread;

use queen::Message;
use nson::msg;

fn handle_client(mut stream: TcpStream) {
    loop {
        // let mut buf = [0u8; 4 * 1024];
        // let size = stream.read(&mut buf).unwrap();

        // if size == 0 {
        //  return;
        // }

        // println!("{:?}", &buf[..size]);
        // stream.write(&buf[..size]).unwrap();

        let message = Message::decode(&mut stream).unwrap();
        println!("{:?}", message);

        let reply = msg!{"ok": true};
        reply.encode(&mut stream).unwrap();
    }
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8888")?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        thread::spawn(|| {
            handle_client(stream.unwrap());
        });
    }
    Ok(())
}
