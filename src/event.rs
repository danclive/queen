use sys;
use token::Token;
use ready::Ready;

pub struct Events {
    pub inner: sys::epoll::Events
}

impl Events {
    #[inline]
    pub fn with_capacity(size: usize) -> Events {
        Events {
            inner: sys::epoll::Events::with_capacity(size)
        }
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<Event> {
        self.inner.get(idx)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Event {
    kind: Ready,
    token: Token
}

impl Event {
    #[inline]
    pub fn new(readiness: Ready, token: Token) -> Event {
        Event {
            kind: readiness,
            token: token
        }
    }

    #[inline]
    pub fn readiness(&self) -> Ready {
        self.kind
    }

    #[inline]
    pub fn token(&self) -> Token {
        self.token
    }
}
