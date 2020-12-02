pub const CHAN:      &str = "_ch";

// channel
pub const ATTACH:      &str = "_ah";
pub const DETACH:      &str = "_dh";
pub const BIND:        &str = "_bi";
pub const UNBIND:      &str = "_ub";
pub const PING:        &str = "_pi";
pub const QUERY:       &str = "_qu";
pub const MINE:        &str = "_mi";
pub const CUSTOM:      &str = "_cu";
pub const CTRL:        &str = "_ct";

// params
pub const SOCKET_ID:   &str = "_so";
pub const SLOT_ID:     &str = "_sl";
pub const VALUE:       &str = "_va";
pub const LABEL:       &str = "_la";
pub const TO:          &str = "_to";
pub const FROM:        &str = "_fr";
pub const SHARE:       &str = "_sh";
pub const ROOT:        &str = "_ro";
pub const ATTR:        &str = "_at";
pub const ADDR:        &str = "_ad";
pub const CHANS:       &str = "_cs";
pub const SHARE_CHANS: &str = "_sc";

// message id
pub const ID:        &str = "_id";

// error
pub const CODE:      &str = "_co";
pub const ERROR:     &str = "_er";

// slot event channel
pub const SLOT_READY:  &str = "_slre";
pub const SLOT_BREAK:  &str = "_slbr";
pub const SLOT_ATTACH: &str = "_slat";
pub const SLOT_DETACH: &str = "_slde";
pub const SLOT_KILL:   &str = "_slki";
pub const SLOT_SEND:   &str = "_slse";
pub const SLOT_RECV:   &str = "_slrc";

// attr
pub const SEND_NUM:    &str = "_snum";
pub const RECV_NUM:    &str = "_rnum";

// crypto
pub const AES_128_GCM:       &str = "A1G";
pub const AES_256_GCM:       &str = "A2G";
pub const CHACHA20_POLY1305: &str = "CP1";

// network
pub const HAND:        &str = "_ha";
pub const KEEP_ALIVE:  &str = "_ke";

pub const METHOD:      &str = "_me";
pub const SECURE:      &str = "_se";
pub const ORIGIN:      &str = "_or";
