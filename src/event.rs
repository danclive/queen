use std::fmt;

use sys;

use {Token, Ready};

pub struct Events {
    pub inner: sys::Events
}

impl Events {
    #[inline]
    pub fn with_capacity(size: usize) -> Events {
        Events {
            inner: sys::Events::with_capacity(size)
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

    pub fn iter(&self) -> Iter {
        Iter {
            inner: self,
            pos: 0
        }
    }
}

impl fmt::Debug for Events {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Events")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
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

#[derive(Debug, Clone)]
pub struct Iter<'a> {
    inner: &'a Events,
    pos: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}

impl<'a> IntoIterator for &'a Events {
    type Item = Event;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub struct IntoIter {
    inner: Events,
    pos: usize
}

impl Iterator for IntoIter {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}

impl IntoIterator for Events {
    type Item = Event;
    type IntoIter = IntoIter;

    fn into_iter(self) -> self::IntoIter {
        IntoIter {
            inner: self,
            pos: 0
        }
    }
}
