use nson::Message;

use crate::queen::Port;

pub struct Callback<T> {
    pub accept_fn: Option<AcceptFn<T>>,
    pub remove_fn: Option<RemoveFn<T>>,
    pub recv_fn: Option<RecvFn<T>>,
    pub send_fn: Option<SendFn<T>>,
    pub auth_fn: Option<AuthFn<T>>,
    pub attach_fn: Option<AttachFn<T>>,
    pub detach_fn: Option<DetachFn<T>>,
    pub emit_fn: Option<EmitFn<T>>,
    pub kill_fn: Option<KillFn<T>>
}

type AcceptFn<T> = Box<dyn Fn(&Port, &mut T) -> bool + Send>;
type RemoveFn<T> = Box<dyn Fn(&Port, &mut T) + Send>;
type RecvFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) -> bool + Send>;
type SendFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) -> bool + Send>;
type AuthFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) -> bool + Send>;
type AttachFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) -> bool + Send>;
type DetachFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) + Send>;
type EmitFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) -> bool + Send>;
type KillFn<T> = Box<dyn Fn(&Port, &mut Message, &mut T) -> bool + Send>;

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
            kill_fn: None
        }
    }

    pub fn accept<F>(&mut self, f: F) where F: Fn(&Port, &mut T) -> bool + Send + 'static {
        self.accept_fn = Some(Box::new(f))
    }

    pub fn remove<F>(&mut self, f: F) where F: Fn(&Port, &mut T) + Send + 'static {
        self.remove_fn = Some(Box::new(f))
    }

    pub fn recv<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) -> bool + Send + 'static {
        self.recv_fn = Some(Box::new(f))
    }

    pub fn send<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) -> bool + Send + 'static {
        self.send_fn = Some(Box::new(f))
    }

    pub fn auth<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) -> bool + Send + 'static {
        self.auth_fn = Some(Box::new(f))
    }

    pub fn attach<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) -> bool + Send + 'static {
        self.attach_fn = Some(Box::new(f))
    }

    pub fn detach<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) + Send + 'static {
        self.detach_fn = Some(Box::new(f))
    }

    pub fn emit<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) -> bool + Send + 'static {
        self.emit_fn = Some(Box::new(f))
    }

    pub fn kill<F>(&mut self, f: F) where F: Fn(&Port, &mut Message, &mut T) -> bool+ Send + 'static {
        self.kill_fn = Some(Box::new(f))
    }
}

impl<T> Default for Callback<T> {
    fn default() -> Callback<T> {
        Callback::new()
    }
}
