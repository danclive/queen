use std::{fmt, ops};

#[derive(Copy, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct Ready(i16);

const READABLE: i16 = 0b0001;
const WRITABLE: i16 = 0b0010;
const ERROR: i16    = 0b0100;
const HUP: i16      = 0b1000;

impl Ready {
    #[inline]
    pub fn empty() -> Ready {
        Ready(0)
    }

    #[inline]
    pub fn readable() -> Ready {
        Ready(READABLE)
    }

    #[inline]
    pub fn writable() -> Ready {
        Ready(WRITABLE)
    }

    #[inline]
    pub fn error() -> Ready {
        Ready(ERROR)
    }

    #[inline]
    pub fn hup() -> Ready {
        Ready(HUP)
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self == Ready::empty()
    }

    #[inline]
    pub fn is_readable(self) -> bool {
        self.contains(Ready::readable())
    }

    #[inline]
    pub fn is_writable(self) -> bool {
        self.contains(Ready::writable())
    }

    #[inline]
    pub fn is_error(self) -> bool {
        self.contains(Ready(ERROR))
    }

    #[inline]
    pub fn is_hup(self) -> bool {
        self.contains(Ready(HUP))
    }

    #[inline]
    pub fn insert(&mut self, other: Ready) {
        self.0 |= other.0;
    }

    #[inline]
    pub fn remove(&mut self, other: Ready) {
        self.0 &= !other.0;
    }

    #[inline]
    pub fn contains(self, other: Ready) -> bool {
        (self & other) == other
    }

    #[inline]
    pub fn as_i16(self) -> i16 {
        self.0
    }
}

impl ops::BitOr for Ready {
    type Output = Ready;

    #[inline]
    fn bitor(self, other: Ready) -> Ready {
        Ready(self.0 | other.0)
    }
}

impl ops::BitXor for Ready {
    type Output = Ready;

    #[inline]
    fn bitxor(self, other: Ready) -> Ready {
        Ready(self.0 ^ other.0)
    }
}

impl ops::BitAnd for Ready {
    type Output = Ready;

    #[inline]
    fn bitand(self, other: Ready) -> Ready {
        Ready(self.0 & other.0)
    }
}

impl ops::Sub for Ready {
    type Output = Ready;

    #[inline]
    fn sub(self, other: Ready) -> Ready {
        Ready(self.0 & !other.0)
    }
}

impl ops::Not for Ready {
    type Output = Ready;

    #[inline]
    fn not(self) -> Ready {
        Ready(!self.0)
    }
}

impl From<i16> for Ready {
    fn from(event: i16) -> Ready {
        Ready(event)
    }
}

impl fmt::Debug for Ready {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let mut one = false;
        let flags = [
            (Ready::readable(), "Readable"),
            (Ready::writable(), "Writable"),
            (Ready(ERROR), "Error"),
            (Ready(HUP), "Hup")];

        write!(fmt, "Ready {{")?;

        for &(flag, msg) in &flags {
            if self.contains(flag) {
                if one { write!(fmt, " | ")? }
                write!(fmt, "{}", msg)?;

                one = true
            }
        }

        write!(fmt, "}}")?;

        Ok(())
    }
}
