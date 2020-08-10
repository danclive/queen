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
    ConnectionAborted(String),
    Disconnected(String),
    BrokenPipe(String),
    AlreadyExists(String),
    InvalidData(String),
    TimedOut(String),
    IoError(io::Error),
    ErrorCode(Code),
    RecvError(RecvError)
}

pub type Result<T> = result::Result<T, Error>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<Code> for Error {
    fn from(err: Code) -> Error {
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
            Error::ConnectionAborted(ref inner) => write!(fmt, "ConnectionAborted: {}", inner),
            Error::Disconnected(ref inner) => write!(fmt, "Disconnected: {}", inner),
            Error::BrokenPipe(ref inner) => write!(fmt, "BrokenPipe: {}", inner),
            Error::AlreadyExists(ref inner) => write!(fmt, "AlreadyExists: {}", inner),
            Error::InvalidData(ref inner) => write!(fmt, "InvalidData: {}", inner),
            Error::TimedOut(ref inner) => write!(fmt, "TimeOut: {}", inner),
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
pub enum Code {
    Ok = 0,
    InternalError,
    UnsupportedFormat,
    Unauthorized,
    AuthenticationFailed,
    PermissionDenied,
    DuplicateSlotId,
    TargetSlotIdNotExist,
    RefuseReceiveMessage,
    CannotGetChanField,
    UnsupportedChan,
    CannotGetValueField,
    CannotGetSlotIdField,
    InvalidSlotIdFieldType,
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

impl Code {
    pub fn code(self) -> i32 {
        self as i32
    }

    pub fn from_i32(code: i32) -> Code {
        if code < 0 || code > Code::UnknownError as i32 {
            return Code::UnknownError
        }

        unsafe { std::mem::transmute(code) }
    }

    pub fn to_str(&self) -> &str {
        match self {
            Code::Ok => "OK",
            Code::InternalError => "InternalError",
            Code::UnsupportedFormat => "UnsupportedFormat",
            Code::Unauthorized => "Unauthorized",
            Code::AuthenticationFailed => "AuthenticationFailed",
            Code::PermissionDenied => "PermissionDenied",
            Code::DuplicateSlotId => "DuplicatePortId",
            Code::TargetSlotIdNotExist => "TargetPortIdNotExist",
            Code::RefuseReceiveMessage => "RefuseReceiveMessage",
            Code::CannotGetChanField => "CannotGetChanField",
            Code::UnsupportedChan => "UnsupportedChan",
            Code::CannotGetValueField => "CannotGetValueField",
            Code::CannotGetSlotIdField => "CannotGetSlotIdField",
            Code::InvalidSlotIdFieldType => "InvalidPortIdFieldType",
            Code::InvalidLabelFieldType => "InvalidLabelFieldType",
            Code::InvalidToFieldType => "InvalidToFieldType",
            Code::InvalidRootFieldType => "InvalidRootFieldType",
            Code::EmptyFieldName => "EmptyFieldName",
            Code::EmptyFieldValue => "EmptyFieldValue",
            Code::KeyTooLong => "KeyTooLong",
            Code::BadValue => "BadValue",
            Code::NotFound => "NotFound",
            Code::UnknownError => "UnknownError"
        }
    }

    pub fn set(self, message: &mut Message) {
        let code = self.code();
        message.insert(CODE, code);
    }

    pub fn get(message: &Message) -> Option<Code> {
        if let Ok(code) = message.get_i32(CODE) {
            if code < 0 || code > Code::UnknownError as i32 {
                return Some(Code::UnknownError)
            }

            return Some(unsafe { std::mem::transmute(code) });
        }

        None
    }
}

impl fmt::Display for Code {
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
