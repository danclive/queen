// node
pub const CHAN:      &str = "_chan";
pub const CHANS:     &str = "_chas";
pub const AUTH:      &str = "_auth";
pub const ATTACH:    &str = "_atta";
pub const DETACH:    &str = "_deta";
pub const PING:      &str = "_ping";
pub const QUERY:     &str = "_quer";
pub const NODE_ID:   &str = "_noid";
pub const CLIENT_ID: &str = "_clid";
pub const VALUE:     &str = "_valu";
pub const LABEL:     &str = "_labe";
pub const TO:        &str = "_to";
pub const FROM:      &str = "_from";
pub const SHARE:     &str = "_shar";
pub const ACK:       &str = "_ack";
pub const SUPER:     &str = "_supe";
pub const ATTR:      &str = "_attr";
pub const CUSTOM:    &str = "_cust";

// message
pub const ID:        &str = "_id";
pub const ADDR:      &str = "_addr";

// error
pub const OK:    &str = "ok";
pub const ERROR: &str = "error";

// client event
pub const CLIENT_READY:  &str = "_ptre";
pub const CLIENT_BREAK:  &str = "_ptbr";
pub const CLIENT_ATTACH: &str = "_ptat";
pub const CLIENT_DETACH: &str = "_ptde";
pub const CLIENT_KILL:   &str = "_ptki";
pub const CLIENT_SEND:   &str = "_ptse";
pub const CLIENT_RECV:   &str = "_ptrc";

// query
pub const QUERY_PORT_NUM: &str = "$port_num";
pub const QUERY_CHAN_NUM: &str = "$chan_num";
pub const QUERY_PORTS:    &str = "$ports";
pub const QUERY_PORT:     &str = "$port";

// crypto
pub const AES_128_GCM:       &str = "AES_128_GCM";
pub const AES_256_GCM:       &str = "AES_256_GCM";
pub const CHACHA20_POLY1305: &str = "CHACHA20_POLY1305";

// network
pub const HANDSHAKE: &str = "_hand";
pub const METHOD:    &str = "_meth";
pub const ACCESS:    &str = "_acce";

// client
pub const ATTACH_ID:    &str = "_atid";
pub const REQUEST_ID:   &str = "_reid";
pub const RECV:         &str = "RECV";
pub const CALL:         &str = "CALL";
pub const UNKNOWN:      &str = "_unknown";
