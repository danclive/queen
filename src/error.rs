use nson::Message;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(i32)]
pub enum ErrorCode {
    OK = 0,
    InternalError,
    UnsupportedFormat,
    Unauthorized,
    AuthenticationFailed,
    ClientNotExist,
    NoConsumers,
    DuplicateClientId,
    RefuseReceiveMessage,
    CannotGetChanField,
    InvalidChanField,
    UnsupportedChan,
    CannotGetTimeidField,
    CannotGetValueField,
    InvalidIdField,
    EmptyFieldName,
    EmptyFieldValue,
    KeyTooLong,
    BadValue,
    UnknownError,
}

impl ErrorCode {
    pub fn code(&self) -> i32 {
        *self as i32
    }

    pub fn to_str(&self) -> &str{
        match self {
            ErrorCode::OK => "OK",
            ErrorCode::InternalError => "InternalError",
            ErrorCode::UnsupportedFormat => "UnsupportedFormat",
            ErrorCode::Unauthorized => "Unauthorized",
            ErrorCode::AuthenticationFailed => "AuthenticationFailed",
            ErrorCode::ClientNotExist => "ClientNotExist",
            ErrorCode::NoConsumers => "NoConsumers",
            ErrorCode::DuplicateClientId => "DuplicateClientId",
            ErrorCode::RefuseReceiveMessage => "RefuseReceiveMessage",
            ErrorCode::CannotGetChanField => "CannotGetChanField",
            ErrorCode::InvalidChanField => "InvalidChanField",
            ErrorCode::UnsupportedChan => "UnsupportedChan",
            ErrorCode::CannotGetTimeidField => "CannotGetTimeidField",
            ErrorCode::CannotGetValueField => "CannotGetValueField",
            ErrorCode::InvalidIdField => "InvalidIdField",
            ErrorCode::EmptyFieldName => "EmptyFieldName",
            ErrorCode::EmptyFieldValue => "EmptyFieldValue",
            ErrorCode::KeyTooLong => "KeyTooLong",
            ErrorCode::BadValue => "BadValue",
            ErrorCode::UnknownError => "UnknownError"
        }
    }

    pub fn to_message(self, msg: &mut Message) {
        let code = self.code();
        msg.insert("ok", code);

        if code > 0 {
            msg.insert("error", self.to_str());
        }
    }

    pub fn has_error(msg: &Message) -> Option<ErrorCode> {
        if let Ok(ok) = msg.get_i32("ok") {
            if ok < 0 || ok > ErrorCode::UnknownError as i32 {
                return Some(ErrorCode::UnknownError)
            }

            return Some(unsafe { std::mem::transmute(ok) });
        }

        None
    }
}