extern crate libc;
extern crate nix;

pub mod sys;
pub mod event;
pub mod token;
pub mod ready;
pub mod poll_opt;
pub mod poll;
pub mod evented;
pub mod registration;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}


/*
extern crate soio;
extern crate nix;

use std::io::{Write, Read};

use std::os::unix::io::{IntoRawFd, AsRawFd, FromRawFd, RawFd};

use soio::*;

fn main() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);

    let flags = nix::sys::eventfd::EFD_CLOEXEC | nix::sys::eventfd::EFD_NONBLOCK;

    let eventfd = nix::sys::eventfd::eventfd(0, flags).unwrap();

    //let eventfd_2 = eventfd.clone();

    let mut file = unsafe { sys::Io::from_raw_fd(eventfd) };

    let mut d = [0, 0, 0, 0, 0, 0, 0, 0];
    println!("{:?}", file.read(&mut d));

    //let d = [1, 1, 1, 1, 1, 1, 1, 1];

    //println!("{:?}", file.write(&d));

    //file.flush();
    //println!("{:?}", file);


    poll.register(&EventedFd(&eventfd), Token(123), Ready::readable(), PollOpt::level()).unwrap();

    println!("{:?}", 111);


    ::std::thread::spawn(move ||{
        //let eventfd_2 = eventfd_2;

        let d = [1, 0, 0, 0, 0, 0, 0, 0];

        let mut file = unsafe { sys::Io::from_raw_fd(eventfd) };

        loop {
            //::std::thread::sleep(::std::time::Duration::from_secs(4));

            println!("{:?}", file);
            //eventfd.write();

            file.write(&d).unwrap();
            println!("{:?}", 666666666);
            file.flush().unwrap();
            println!("{:?}", 777777777);

            ::std::thread::sleep(::std::time::Duration::from_secs(4));
        }

    });

    println!("{:?}", 222);

    loop {
        println!("{:?}", 333);
        poll.poll(&mut events, None).unwrap();
        println!("{:?}", 444);

        for event in &events {
            println!("{:?}", event);

            //let mut file = unsafe { sys::Io::from_raw_fd(eventfd_2) };
            let mut d = [0, 0, 0, 0, 0, 0, 0, 0];
            println!("{:?}", file.read(&mut d));
        }
    }
}
*/
