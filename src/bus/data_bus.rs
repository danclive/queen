use std::sync::{Arc, Mutex};
use std::collections::{HashMap};
use std::mem;

use nson::{Message, Value};

#[derive(Clone)]
pub struct DataBus {
    inner: Arc<Mutex<InnerDataBus>>
}

struct InnerDataBus {
    data: HashMap<String, Cell>,
    push: Vec<(String, Cell)>,
    on_set: Option<Arc<Box<dyn Fn(String, Cell)>>>
}

#[derive(Debug, Clone)]
pub struct Cell {
    pub value: Value,
    pub attr: Option<Message>
}

impl DataBus {
    pub fn on_set<F>(&self, f: F) where F: Fn(String, Cell) + 'static {
        let mut inner = self.inner.lock().unwrap();
        inner.on_set = Some(Arc::new(Box::new(f)));
    }

    pub fn set(
        &self,
        key: impl Into<String>,
        value: impl Into<Value>,
        attr: Option<Message>
    ) {
        let mut inner = self.inner.lock().unwrap();

        let key = key.into();

        let cell = if let Some(cell) = inner.data.get_mut(&key) {
            cell.value = value.into();

            if let Some(attr) = attr {
                cell.attr = Some(attr);
            }

            cell.clone()
        } else {
            let cell = Cell { value: value.into(), attr };
            inner.data.insert(key.clone(), cell.clone());

            cell
        };

        inner.push.push((key.clone(), cell.clone()));

        let on_set = inner.on_set.clone();

        drop(inner);

        if let Some(on_set) = on_set {
            on_set(key, cell);
        }
    }

    pub fn remote_set(
        &self,
        key: impl Into<String>,
        value: impl Into<Value>,
        attr: Option<Message>
    ) {
        let mut inner = self.inner.lock().unwrap();

        let key = key.into();

        let cell = if let Some(cell) = inner.data.get_mut(&key) {
            cell.value = value.into();

            if let Some(attr) = attr {
                cell.attr = Some(attr);
            }

            cell.clone()
        } else {
            let cell = Cell { value: value.into(), attr };
            inner.data.insert(key.clone(), cell.clone());

            cell
        };

        let on_set = inner.on_set.clone();

        drop(inner);

        if let Some(on_set) = on_set {
            on_set(key, cell);
        }
    }

    pub fn get(&self, key: &str) -> Option<Cell> {
        let inner = self.inner.lock().unwrap();
        inner.data.get(key).cloned()
    }

    pub fn tick(&self) -> Vec<(String, Cell)> {
        let mut inner = self.inner.lock().unwrap();
        mem::replace(&mut inner.push, vec![])
    }

    pub fn all(&self) -> HashMap<String, Cell> {
        let inner = self.inner.lock().unwrap();
        inner.data.clone()
    }
}
