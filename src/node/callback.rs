use std::usize;
use nson::Message;
use crate::net::Addr;

pub struct Callback<T> {
    pub accept_fn: Option<AcceptFn<T>>,
    pub remove_fn: Option<RemoveFn<T>>,
    pub recv_fn: Option<RecvFn<T>>,
    pub send_fn: Option<SendFn<T>>,
    pub auth_fn: Option<AuthFn<T>>,
    pub attach_fn: Option<AttachFn<T>>,
    pub detach_fn: Option<DetachFn<T>>,
    pub emit_fn: Option<EmitFn<T>>
}

type AcceptFn<T> = Box<dyn Fn(usize, &Addr, &mut T) -> bool>;
type RemoveFn<T> = Box<dyn Fn(usize, &Addr, &mut T)>;
type RecvFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type SendFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type AuthFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type AttachFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;
type DetachFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T)>;
type EmitFn<T> = Box<dyn Fn(usize, &Addr, &mut Message, &mut T) -> bool>;

impl<T> Callback<T> {
    pub fn new() -> Callback<T> {
        Callback {
            accept_fn: None,
            remove_fn: None,
            recv_fn: None,
            send_fn: None,
            auth_fn: None,
            attach_fn: None,
            detach_fn: None,
            emit_fn: None
        }
    }

    pub fn accept<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut T) -> bool + 'static {
        self.accept_fn = Some(Box::new(f))
    }

    pub fn remove<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut T) + 'static {
        self.remove_fn = Some(Box::new(f))
    }

    pub fn recv<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.recv_fn = Some(Box::new(f))
    }

    pub fn send<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.send_fn = Some(Box::new(f))
    }

    pub fn auth<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.auth_fn = Some(Box::new(f))
    }

    pub fn attach<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.attach_fn = Some(Box::new(f))
    }

    pub fn detach<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) + 'static {
        self.detach_fn = Some(Box::new(f))
    }

    pub fn emit<F>(&mut self, f: F) where F: Fn(usize, &Addr, &mut Message, &mut T) -> bool + 'static {
        self.emit_fn = Some(Box::new(f))
    }
}

impl<T> Default for Callback<T> {
    fn default() -> Callback<T> {
        Callback::new()
    }
}
