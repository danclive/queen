use std::{io, result};
use std::error;
use std::fmt;

use nson::Message;

use crate::dict::*;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    ErrorCode(ErrorCode)
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

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IoError(ref inner) => inner.fmt(fmt),
            Error::ErrorCode(ref inner) => inner.fmt(fmt)
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
    NoConsumers,
    DuplicatePortId,
    TargetPortIdNotExist,
    RefuseReceiveMessage,
    CannotGetChanField,
    UnsupportedChan,
    CannotGetValueField,
    InvalidPortIdFieldType,
    InvalidLabelFieldType,
    InvalidNonceFieldType,
    InvalidToFieldType,
    InvalidSuperFieldType,
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
            ErrorCode::NoConsumers => "NoConsumers",
            ErrorCode::DuplicatePortId => "DuplicatePortId",
            ErrorCode::TargetPortIdNotExist => "TargetPortIdNotExist",
            ErrorCode::RefuseReceiveMessage => "RefuseReceiveMessage",
            ErrorCode::CannotGetChanField => "CannotGetChanField",
            ErrorCode::UnsupportedChan => "UnsupportedChan",
            ErrorCode::CannotGetValueField => "CannotGetValueField",
            ErrorCode::InvalidPortIdFieldType => "InvalidPortIdFieldType",
            ErrorCode::InvalidLabelFieldType => "InvalidLabelFieldType",
            ErrorCode::InvalidNonceFieldType => "InvalidNonceFieldType",
            ErrorCode::InvalidToFieldType => "InvalidToFieldType",
            ErrorCode::InvalidSuperFieldType => "InvalidSuperFieldType",
            ErrorCode::EmptyFieldName => "EmptyFieldName",
            ErrorCode::EmptyFieldValue => "EmptyFieldValue",
            ErrorCode::KeyTooLong => "KeyTooLong",
            ErrorCode::BadValue => "BadValue",
            ErrorCode::NotFound => "NotFound",
            ErrorCode::UnknownError => "UnknownError"
        }
    }

    pub fn insert_message(self, message: &mut Message) {
        let code = self.code();
        message.insert(OK, code);

        #[cfg(debug_assertions)]
        {if code > 0 {
            message.insert(ERROR, self.to_str());
        }}
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