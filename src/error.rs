use nson::Message;

use crate::dict::*;

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
    InvalidChanField,
    UnsupportedChan,
    CannotGetTimeidField,
    TimeidNotExist,
    CannotGetValueField,
    InvalidIdField,
    InvalidFieldType,
    EmptyFieldName,
    EmptyFieldValue,
    KeyTooLong,
    BadValue,
    UnknownError,
}

impl ErrorCode {
    pub fn code(self) -> i32 {
        self as i32
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
            ErrorCode::InvalidChanField => "InvalidChanField",
            ErrorCode::UnsupportedChan => "UnsupportedChan",
            ErrorCode::CannotGetTimeidField => "CannotGetTimeidField",
            ErrorCode::TimeidNotExist => "TimeidNotExist",
            ErrorCode::CannotGetValueField => "CannotGetValueField",
            ErrorCode::InvalidIdField => "InvalidIdField",
            ErrorCode::InvalidFieldType => "InvalidFieldType",
            ErrorCode::EmptyFieldName => "EmptyFieldName",
            ErrorCode::EmptyFieldValue => "EmptyFieldValue",
            ErrorCode::KeyTooLong => "KeyTooLong",
            ErrorCode::BadValue => "BadValue",
            ErrorCode::UnknownError => "UnknownError"
        }
    }

    pub fn insert_message(self, msg: &mut Message) {
        let code = self.code();
        msg.insert(OK, code);

        #[cfg(debug_assertions)]
        {if code > 0 {
            msg.insert(ERROR, self.to_str());
        }}
    }

    pub fn has_error(msg: &Message) -> Option<ErrorCode> {
        if let Ok(ok) = msg.get_i32(OK) {
            if ok < 0 || ok > ErrorCode::UnknownError as i32 {
                return Some(ErrorCode::UnknownError)
            }

            return Some(unsafe { std::mem::transmute(ok) });
        }

        None
    }
}
