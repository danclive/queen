#![allow(dead_code)]

use nson::Message;

pub struct Point {

}

struct InnerPoint {

}

impl Point {
    pub fn bind() {

    }

    pub fn connect() {

    }

    pub fn call(
        _service: &str,
        _method: &str,
        _request: Message
    ) -> Message {
        unimplemented!()
    }

    pub fn add(
        &mut self,
        _service: &str,
        _method: &str,
        _handle: impl Handle + 'static
    ) {
    
    }
}

fn aaa() {
    // let mut point = Point {
    //     // methods: Vec::new()
    // };

    // point.register(HandleA);
    // point.register(HandleB);
}

impl InnerPoint {

}

pub trait Handle {
    fn call(&mut self, req: Message) -> Message;
}

struct HandleA;

impl Handle for HandleA {
    fn call(&mut self, req: Message) -> Message {
        req
    }
}

struct HandleB;

impl Handle for HandleB {
    fn call(&mut self, req: Message) -> Message {
        req
    }
}
