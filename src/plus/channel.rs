use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::error;
use std::any::Any;
use std::fmt;

use sys::io;
use {Registration, Ready, Evented, Poll, Token, PollOpt};

pub fn channel<T>() -> io::Result<(Sender<T>, Receiver<T>)> {
    let (tx_ctl, rx_ctl) = ctl_pair()?;
    let (tx, rx) = mpsc::channel();

    let tx = Sender {
        tx: tx,
        ctl: tx_ctl
    };

    let rx = Receiver {
        rx: rx,
        ctl: rx_ctl
    };

    Ok((tx, rx))
}

pub fn sync_channel<T>(bound: usize) -> io::Result<(SyncSender<T>, Receiver<T>)> {
    let (tx_ctl, rx_ctl) = ctl_pair()?;
    let (tx, rx) = mpsc::sync_channel(bound);

    let tx = SyncSender {
        tx: tx,
        ctl: tx_ctl
    };

    let rx = Receiver {
        rx: rx,
        ctl: rx_ctl
    };

    Ok((tx, rx))
}

pub fn ctl_pair() -> io::Result<(SenderCtl, ReceiverCtl)> {
    let registration = Registration::new()?;

    let inner = Arc::new(Inner {
        pending: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        registration: registration
    });

    let tx = SenderCtl {
        inner: inner.clone()
    };

    let rx = ReceiverCtl {
        inner: inner
    };

    Ok((tx, rx))
}

#[derive(Clone)]
pub struct Sender<T> {
    tx: mpsc::Sender<T>,
    ctl: SenderCtl
}

#[derive(Clone)]
pub struct SyncSender<T> {
    tx: mpsc::SyncSender<T>,
    ctl: SenderCtl
}

pub struct Receiver<T> {
    rx: mpsc::Receiver<T>,
    ctl: ReceiverCtl
}

//#[derive(Clone)]
pub struct SenderCtl {
    inner: Arc<Inner>
}

pub struct ReceiverCtl {
    inner: Arc<Inner>
}

struct Inner {
    pending: AtomicUsize,
    senders: AtomicUsize,
    registration: Registration
}

pub enum SendError<T> {
    Io(io::Error),
    Disconnected(T),
}

pub enum TrySendError<T> {
    Io(io::Error),
    Full(T),
    Disconnected(T),
}

impl<T> Sender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.tx.send(t).map_err(SendError::from).and_then(|_| { self.ctl.inc()?; Ok(()) })
    }
}

impl<T> SyncSender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.tx.send(t).map_err(From::from).and_then(|_| { self.ctl.inc()?; Ok(()) })
    }

    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(t).map_err(From::from).and_then(|_| { self.ctl.inc()?; Ok(()) })
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, mpsc::TryRecvError> {
        self.rx.try_recv().and_then(|res| {
            let _ = self.ctl.dec();
            Ok(res)
        })
    }
}

impl<T> Evented for Receiver<T> {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.ctl.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.ctl.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.ctl.deregister(poll)
    }
}

impl SenderCtl {
    pub fn inc(&self) -> io::Result<()> {
        let cnt = self.inner.pending.fetch_add(1, Ordering::Acquire);

        if 0 == cnt {
            self.inner.registration.set_readiness(Ready::readable())?;
        }

        Ok(())
    }
}

impl Clone for SenderCtl {
    fn clone(&self) -> SenderCtl {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        SenderCtl { inner: self.inner.clone() }
    }
}

impl Drop for SenderCtl {
    fn drop(&mut self) {
        if self.inner.senders.fetch_sub(1, Ordering::Release) == 1 {
            let _ = self.inc();
        }
    }
}

impl ReceiverCtl {
    pub fn dec(&self) -> io::Result<()> {
        let first = self.inner.pending.load(Ordering::Acquire);

        if first == 1 {
            self.inner.registration.set_readiness(Ready::empty())?;
        }

        let second = self.inner.pending.fetch_sub(1, Ordering::AcqRel);

        if first == 1 && second > 1 {
            self.inner.registration.set_readiness(Ready::readable())?;
        }

        Ok(())
    }
}

impl Evented for ReceiverCtl {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.inner.registration.register(poll, token, interest, opts)?;

        if self.inner.pending.load(Ordering::Relaxed) > 0 {
            self.inner.registration.set_readiness(Ready::readable())?;
        }

        Ok(())
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.inner.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.inner.registration.deregister(poll)
    }
}




impl<T> From<mpsc::SendError<T>> for SendError<T> {
    fn from(src: mpsc::SendError<T>) -> SendError<T> {
        SendError::Disconnected(src.0)
    }
}

impl<T> From<io::Error> for SendError<T> {
    fn from(src: io::Error) -> SendError<T> {
        SendError::Io(src)
    }
}

impl<T> From<mpsc::TrySendError<T>> for TrySendError<T> {
    fn from(src: mpsc::TrySendError<T>) -> TrySendError<T> {
        match src {
            mpsc::TrySendError::Full(v) => TrySendError::Full(v),
            mpsc::TrySendError::Disconnected(v) => TrySendError::Disconnected(v),
        }
    }
}

impl<T> From<mpsc::SendError<T>> for TrySendError<T> {
    fn from(src: mpsc::SendError<T>) -> TrySendError<T> {
        TrySendError::Disconnected(src.0)
    }
}

impl<T> From<io::Error> for TrySendError<T> {
    fn from(src: io::Error) -> TrySendError<T> {
        TrySendError::Io(src)
    }
}

impl<T: Any> error::Error for SendError<T> {
    fn description(&self) -> &str {
        match self {
            &SendError::Io(ref io_err) => io_err.description(),
            &SendError::Disconnected(..) => "Disconnected",
        }
    }
}

impl<T: Any> error::Error for TrySendError<T> {
    fn description(&self) -> &str {
        match self {
            &TrySendError::Io(ref io_err) => io_err.description(),
            &TrySendError::Full(..) => "Full",
            &TrySendError::Disconnected(..) => "Disconnected",
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_send_error(self, f)
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_send_error(self, f)
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_try_send_error(self, f)
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_try_send_error(self, f)
    }
}

#[inline]
fn format_send_error<T>(e: &SendError<T>, f: &mut fmt::Formatter) -> fmt::Result {
    match e {
        &SendError::Io(ref io_err) => write!(f, "{}", io_err),
        &SendError::Disconnected(..) => write!(f, "Disconnected"),
    }
}

#[inline]
fn format_try_send_error<T>(e: &TrySendError<T>, f: &mut fmt::Formatter) -> fmt::Result {
    match e {
        &TrySendError::Io(ref io_err) => write!(f, "{}", io_err),
        &TrySendError::Full(..) => write!(f, "Full"),
        &TrySendError::Disconnected(..) => write!(f, "Disconnected"),
    }
}
