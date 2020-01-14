use nson::Message;

use crate::queen::{Sessions, Session};

pub struct Callback<T> {
    pub accept_fn: Option<Fn1<T>>,
    pub remove_fn: Option<Fn2<T>>,
    pub recv_fn: Option<Fn3<T>>,
    pub send_fn: Option<Fn3<T>>,
    pub auth_fn: Option<Fn3<T>>,
    pub attach_fn: Option<Fn3<T>>,
    pub detach_fn: Option<Fn4<T>>,
    pub emit_fn: Option<Fn3<T>>,
    pub push_fn: Option<Fn3<T>>,
    pub kill_fn: Option<Fn3<T>>,
    pub custom_fn: Option<Fn5<T>>
}

type Fn1<T> = Box<dyn Fn(&Session, &T) -> bool + Send>;
type Fn2<T> = Box<dyn Fn(&Session, &T) + Send>;
type Fn3<T> = Box<dyn Fn(&Session, &mut Message, &T) -> bool + Send>;
type Fn4<T> = Box<dyn Fn(&Session, &mut Message, &T) + Send>;
type Fn5<T> = Box<dyn Fn(&Sessions, usize, &mut Message, &T) + Send>;

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
            emit_fn: None,
            push_fn: None,
            kill_fn: None,
            custom_fn: None
        }
    }

    pub fn accept<F>(&mut self, f: F) where F: Fn(&Session, &T) -> bool + Send + 'static {
        self.accept_fn = Some(Box::new(f))
    }

    pub fn remove<F>(&mut self, f: F) where F: Fn(&Session, &T) + Send + 'static {
        self.remove_fn = Some(Box::new(f))
    }

    pub fn recv<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.recv_fn = Some(Box::new(f))
    }

    pub fn send<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.send_fn = Some(Box::new(f))
    }

    pub fn auth<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.auth_fn = Some(Box::new(f))
    }

    pub fn attach<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.attach_fn = Some(Box::new(f))
    }

    pub fn detach<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) + Send + 'static {
        self.detach_fn = Some(Box::new(f))
    }

    pub fn emit<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.emit_fn = Some(Box::new(f))
    }

    pub fn push<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.push_fn = Some(Box::new(f))
    }

    pub fn kill<F>(&mut self, f: F) where F: Fn(&Session, &mut Message, &T) -> bool + Send + 'static {
        self.kill_fn = Some(Box::new(f))
    }

    pub fn custom<F>(&mut self, f: F) where F: Fn(&Sessions, usize, &mut Message, &T) + Send + 'static {
        self.custom_fn = Some(Box::new(f))
    }
}

impl<T> Default for Callback<T> {
    fn default() -> Callback<T> {
        Callback::new()
    }
}
