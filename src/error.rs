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
    Full(String),
    Disconnected(String),
    BrokenPipe(String),
    AlreadyExists(String),
    InvalidData(String),
    Empty(String),
    TimedOut(String),
    Exit(String),
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
            Error::Full(ref inner) => write!(fmt, "Full: {}", inner),
            Error::Disconnected(ref inner) => write!(fmt, "Disconnected: {}", inner),
            Error::BrokenPipe(ref inner) => write!(fmt, "BrokenPipe: {}", inner),
            Error::AlreadyExists(ref inner) => write!(fmt, "AlreadyExists: {}", inner),
            Error::InvalidData(ref inner) => write!(fmt, "InvalidData: {}", inner),
            Error::Empty(ref inner) => write!(fmt, "Empty: {}", inner),
            Error::TimedOut(ref inner) => write!(fmt, "TimeOut: {}", inner),
            Error::Exit(ref inner) => write!(fmt, "Exit: {}", inner),
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

    Unauthorized = 100,
    AuthenticationFailed = 101,
    PermissionDenied = 102,

    DuplicateSlotId = 200,
    TargetSlotIdNotExist = 201,
    CannotGetChanField = 202,
    UnsupportedChan = 203,
    CannotGetValueField = 204,
    CannotGetSlotIdField = 205,
    InvalidSlotIdFieldType = 206,
    InvalidToFieldType = 207,
    InvalidRootFieldType = 208,
    InvalidShareFieldType = 209,
    InvalidToSocketFieldType = 210,

    InternalError = 300,
    UnsupportedFormat = 301,
    EmptyFieldName = 302,
    EmptyFieldValue = 303,
    KeyTooLong = 304,
    BadValue = 305,
    NotFound = 306,

    UnknownError = -1,
}

impl Code {
    pub fn code(self) -> i32 {
        self as i32
    }

    pub fn from_i32(code: i32) -> Code {
        match code {
            0 => Code::Ok,

            100 => Code::Unauthorized,
            101 => Code::AuthenticationFailed,
            102 => Code::PermissionDenied,

            200 => Code::DuplicateSlotId,
            201 => Code::TargetSlotIdNotExist,
            202 => Code::CannotGetChanField,
            203 => Code::UnsupportedChan,
            204 => Code::CannotGetValueField,
            205 => Code::CannotGetSlotIdField,
            206 => Code::InvalidSlotIdFieldType,
            207 => Code::InvalidToFieldType,
            208 => Code::InvalidRootFieldType,
            209 => Code::InvalidShareFieldType,
            210 => Code::InvalidToSocketFieldType,

            300 => Code::InternalError,
            301 => Code::UnsupportedFormat,
            302 => Code::EmptyFieldName,
            303 => Code::EmptyFieldValue,
            304 => Code::KeyTooLong,
            305 => Code::BadValue,
            306 => Code::NotFound,

            _ => Code::UnknownError
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            Code::Ok => "OK",

            Code::Unauthorized => "Unauthorized",
            Code::AuthenticationFailed => "AuthenticationFailed",
            Code::PermissionDenied => "PermissionDenied",

            Code::DuplicateSlotId => "DuplicatePortId",
            Code::TargetSlotIdNotExist => "TargetPortIdNotExist",
            Code::CannotGetChanField => "CannotGetChanField",
            Code::UnsupportedChan => "UnsupportedChan",
            Code::CannotGetValueField => "CannotGetValueField",
            Code::CannotGetSlotIdField => "CannotGetSlotIdField",
            Code::InvalidSlotIdFieldType => "InvalidPortIdFieldType",
            Code::InvalidToFieldType => "InvalidToFieldType",
            Code::InvalidRootFieldType => "InvalidRootFieldType",
            Code::InvalidShareFieldType => "InvalidShareFieldType",
            Code::InvalidToSocketFieldType => "InvalidToSocketFieldType",

            Code::InternalError => "InternalError",
            Code::UnsupportedFormat => "UnsupportedFormat",
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
            return Some(Code::from_i32(code))
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
