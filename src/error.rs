use std::io;
use std::result;
use std::fmt;
use std::error;
use std::string;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    ResponseError(String),
    FromUtf8Error(string::FromUtf8Error),
}

impl<'a> From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(err: string::FromUtf8Error) -> Error {
        Error::FromUtf8Error(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IoError(ref inner) => inner.fmt(fmt),
            Error::ResponseError(ref inner) => inner.fmt(fmt),
            Error::FromUtf8Error(ref inner) => inner.fmt(fmt)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IoError(ref inner) => inner.description(),
            Error::ResponseError(ref inner) => inner,
            Error::FromUtf8Error(ref inner) => inner.description()
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref inner) => Some(inner),
            Error::ResponseError(_) => None,
            Error::FromUtf8Error(ref inner) => Some(inner)
        }
    }
}
