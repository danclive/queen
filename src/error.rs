use std::io;
use std::{error, result};
use std::fmt;

use nson::Message;

use crate::dict::*;

#[derive(Debug)]
pub enum Error {
    NotFound(String),
    PermissionDenied(String),
    ConnectionRefused(String),
    ConnectionReset(String),
    ConnectionAborted(String),
    NotConnected(String),
    Disconnected(String),
    BrokenPipe(String),
    AlreadyExists(String),
    InvalidInput(String),
    InvalidData(String),
    TimedOut(String),
    Empty(String),
    Full(String),
    Other(String),
    SendError(String),
    IoError(io::Error),
    ErrorCode(ErrorCode),
    RecvError(RecvError)
}

pub type Result<T> = result::Result<T, Error>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<ErrorCode> for Error {
    fn from(err: ErrorCode) -> Error {
        Error::ErrorCode(err)
    }
}

impl From<RecvError> for Error {
    fn from(err: RecvError) -> Error {
        Error::RecvError(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotFound(ref inner) => write!(fmt, "NotFound: {}", inner),
            Error::PermissionDenied(ref inner) => write!(fmt, "PermissionDenied: {}", inner),
            Error::ConnectionRefused(ref inner) => write!(fmt, "ConnectionRefused: {}", inner),
            Error::ConnectionReset(ref inner) => write!(fmt, "ConnectionReset: {}", inner),
            Error::ConnectionAborted(ref inner) => write!(fmt, "ConnectionAborted: {}", inner),
            Error::NotConnected(ref inner) => write!(fmt, "NotConnected: {}", inner),
            Error::Disconnected(ref inner) => write!(fmt, "Disconnected: {}", inner),
            Error::BrokenPipe(ref inner) => write!(fmt, "BrokenPipe: {}", inner),
            Error::AlreadyExists(ref inner) => write!(fmt, "AlreadyExists: {}", inner),
            Error::InvalidInput(ref inner) => write!(fmt, "InvalidInput: {}", inner),
            Error::InvalidData(ref inner) => write!(fmt, "InvalidData: {}", inner),
            Error::TimedOut(ref inner) => write!(fmt, "TimeOut: {}", inner),
            Error::Empty(ref inner) => write!(fmt, "Empty: {}", inner),
            Error::Full(ref inner) => write!(fmt, "Full: {}", inner),
            Error::Other(ref inner) => write!(fmt, "Other: {}", inner),
            Error::SendError(ref inner) => write!(fmt, "SendError: {}", inner),
            Error::IoError(ref inner) => inner.fmt(fmt),
            Error::ErrorCode(ref inner) => inner.fmt(fmt),
            Error::RecvError(ref inner) => inner.fmt(fmt)
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::IoError(ref inner) => Some(inner),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(i32)]
pub enum ErrorCode {
    OK = 0,
    InternalError,
    UnsupportedFormat,
    Unauthorized,
    AuthenticationFailed,
    PermissionDenied,
    NoConsumers,
    DuplicateClientId,
    TargetClientIdNotExist,
    RefuseReceiveMessage,
    CannotGetChanField,
    UnsupportedChan,
    CannotGetValueField,
    CannotGetClientIdField,
    InvalidClientIdFieldType,
    InvalidLabelFieldType,
    InvalidToFieldType,
    InvalidRootFieldType,
    EmptyFieldName,
    EmptyFieldValue,
    KeyTooLong,
    BadValue,
    NotFound,
    UnknownError,
}

impl ErrorCode {
    pub fn code(self) -> i32 {
        self as i32
    }

    pub fn from_i32(code: i32) -> ErrorCode {
        if code < 0 || code > ErrorCode::UnknownError as i32 {
            return ErrorCode::UnknownError
        }

        unsafe { std::mem::transmute(code) }
    }

    pub fn to_str(&self) -> &str{
        match self {
            ErrorCode::OK => "OK",
            ErrorCode::InternalError => "InternalError",
            ErrorCode::UnsupportedFormat => "UnsupportedFormat",
            ErrorCode::Unauthorized => "Unauthorized",
            ErrorCode::AuthenticationFailed => "AuthenticationFailed",
            ErrorCode::PermissionDenied => "PermissionDenied",
            ErrorCode::NoConsumers => "NoConsumers",
            ErrorCode::DuplicateClientId => "DuplicatePortId",
            ErrorCode::TargetClientIdNotExist => "TargetPortIdNotExist",
            ErrorCode::RefuseReceiveMessage => "RefuseReceiveMessage",
            ErrorCode::CannotGetChanField => "CannotGetChanField",
            ErrorCode::UnsupportedChan => "UnsupportedChan",
            ErrorCode::CannotGetValueField => "CannotGetValueField",
            ErrorCode::CannotGetClientIdField => "CannotGetClientIdField",
            ErrorCode::InvalidClientIdFieldType => "InvalidPortIdFieldType",
            ErrorCode::InvalidLabelFieldType => "InvalidLabelFieldType",
            ErrorCode::InvalidToFieldType => "InvalidToFieldType",
            ErrorCode::InvalidRootFieldType => "InvalidRootFieldType",
            ErrorCode::EmptyFieldName => "EmptyFieldName",
            ErrorCode::EmptyFieldValue => "EmptyFieldValue",
            ErrorCode::KeyTooLong => "KeyTooLong",
            ErrorCode::BadValue => "BadValue",
            ErrorCode::NotFound => "NotFound",
            ErrorCode::UnknownError => "UnknownError"
        }
    }

    pub fn insert(self, message: &mut Message) {
        let code = self.code();
        message.insert(OK, code);
    }

    pub fn has_error(message: &Message) -> Option<ErrorCode> {
        if let Ok(ok) = message.get_i32(OK) {
            if ok < 0 || ok > ErrorCode::UnknownError as i32 {
                return Some(ErrorCode::UnknownError)
            }

            return Some(unsafe { std::mem::transmute(ok) });
        }

        None
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "code: {}, error: {}", self.code(), self.to_str())
    }
}


#[derive(PartialEq, Eq, Clone, Copy)]
pub enum SendError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SendError::Full(..) => "Full(..)".fmt(f),
            SendError::Disconnected(..) => "Disconnected(..)".fmt(f),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SendError::Full(..) => "sending on a full channel".fmt(f),
            SendError::Disconnected(..) => "sending on a closed channel".fmt(f),
        }
    }
}

impl<T> error::Error for SendError<T> {}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum RecvError {
    Empty,
    Disconnected,
    TimedOut
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            RecvError::Empty => "receiving on an empty channel".fmt(f),
            RecvError::Disconnected => "receiving on a closed channel".fmt(f),
            RecvError::TimedOut => "receiving timeout".fmt(f)
        }
    }
}

impl error::Error for RecvError {}
