use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::fmt;
use std::thread;

use nson::Value;
use queen_io::plus::block_queue::BlockQueue;

#[derive(Clone)]
pub struct Center {
    inner: Arc<InnerCenter>
}

pub struct Context<'a> {
    pub center: &'a Center,
    pub key: &'a str,
    pub old_value: Option<Value>,
    pub value: Value
}

struct InnerCenter {
    queue: BlockQueue<(String, Option<Value>, Value)>,
    map: Mutex<HashMap<String, Value>>,
    handles: RwLock<HashMap<String, Vec<Box<dyn Fn(Context) + Send + Sync + 'static>>>>,
}

impl Center {
    pub fn new() -> Center {
        Center {
            inner: Arc::new(InnerCenter {
                queue: BlockQueue::with_capacity(4 * 1000),
                map: Mutex::new(HashMap::new()),
                handles: RwLock::new(HashMap::new())
            })
        }
    }

    pub fn insert(&self, key: &str, value: Value) {
        let result = {
            let mut map = self.inner.map.lock().unwrap();
            map.insert(key.to_owned(), value.clone())
        };

        if let Some(ref result) = result {
            if *result == value {
                return
            }
        }

        self.inner.queue.push((key.to_owned(), result, value));
    }

    pub fn get(&self, key: &str) -> Option<Value>{
        let map = self.inner.map.lock().unwrap();
        map.get(key).map(|v| v.clone())
    }

    pub fn remove(&self, key: &str) -> Option<Value> {
        let mut map = self.inner.map.lock().unwrap();
        map.remove(key)
    }

    pub fn on(&self, key: &str, handle: impl Fn(Context) + Send + Sync + 'static) {
        let mut handles = self.inner.handles.write().unwrap();

        let vector = handles.entry(key.to_owned()).or_insert(vec![]);
        vector.push(Box::new(handle));
    }

    pub fn run(&self, worker_size: usize, block: bool) {
        let mut threads = Vec::new();

        for _ in 0..worker_size {
            let that = self.clone();
            threads.push(thread::Builder::new().name("worker".into()).spawn(move || {
                loop {
                    let (key, old_value, value) = that.inner.queue.pop();

                    let handles = that.inner.handles.read().unwrap();
                    if let Some(vector) = handles.get(&key) {
                        for handle in vector {
                            let context = Context {
                                center: &that,
                                key: &key,
                                old_value: old_value.clone(),
                                value: value.clone()
                            };

                            handle(context);
                        }
                    }
                }
            }).unwrap());
        }

        if block {
            for thread in threads {
                thread.join().unwrap();
            }
        }
    }
}

impl<'a> fmt::Debug for Context<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Context {{ key: {}, old_value: {:?}, value: {:?} }}", self.key, self.old_value, self.value)
    }
}

impl<'a> fmt::Display for Context<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Context {{ key: {}, old_value: {:?}, value: {:?} }}", self.key, self.old_value, self.value)
    }
}
