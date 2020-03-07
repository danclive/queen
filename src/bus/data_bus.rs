use std::sync::{Arc, Mutex};
use std::collections::{HashMap};
use std::{mem, fmt};

use nson::{Message, Value};

#[derive(Clone)]
pub struct DataBus {
    inner: Arc<Mutex<InnerDataBus>>
}

impl fmt::Debug for DataBus {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let inner = self.inner.lock().unwrap();

        fmt.debug_struct("DataBus")
            .field("data", &inner.data)
            .field("push", &inner.push)
            .finish()
    }
}

struct InnerDataBus {
    data: HashMap<String, Cell>,
    push: Vec<PushCell>,
    on_set: Option<Arc<Box<dyn Fn(String, Value, Option<Message>, bool) + Sync + Send>>>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    pub value: Value,
    pub attr: Option<Message>
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushCell {
    pub key: String,
    pub value: Value,
    pub attr: Option<Message>
}

impl DataBus {
    pub fn new() -> Self {
        DataBus {
            inner: Arc::new(Mutex::new(InnerDataBus {
                data: HashMap::new(),
                push: Vec::new(),
                on_set: None
            }))
        }
    }

    pub fn on_set<F>(&self, f: F)
        where F: Fn(String, Value, Option<Message>, bool) + Sync + Send + 'static 
    {
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

        inner.push.push(PushCell {
            key: key.clone(),
            value: cell.value.clone(),
            attr: cell.attr.clone()
        });

        let on_set = inner.on_set.clone();

        drop(inner);

        if let Some(on_set) = on_set {
            on_set(key, cell.value, cell.attr, false);
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
            on_set(key, cell.value, cell.attr, true);
        }
    }

    pub fn get(&self, key: &str) -> Option<Cell> {
        let inner = self.inner.lock().unwrap();
        inner.data.get(key).cloned()
    }

    pub fn remove(&self, key: &str) -> Option<Cell> {
        let mut inner = self.inner.lock().unwrap();
        inner.data.remove(key)
    }

    pub fn tick(&self) -> Vec<PushCell> {
        let mut inner = self.inner.lock().unwrap();
        mem::replace(&mut inner.push, vec![])
    }

    pub fn all(&self) -> HashMap<String, Cell> {
        let inner = self.inner.lock().unwrap();
        inner.data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{DataBus, Cell, PushCell};
    use crate::nson::msg;

    #[test]
    fn set_get() {
        let data_bus = DataBus::new();

        data_bus.set("aaa", 123, None);

        assert_eq!(data_bus.get("bbb"), None);
        assert_eq!(data_bus.get("aaa"), Some(Cell { value: 123.into(), attr: None }));

        assert_eq!(data_bus.remove("bbb"), None);
        assert_eq!(data_bus.remove("aaa"), Some(Cell { value: 123.into(), attr: None }));

        assert_eq!(
            data_bus.tick(),
            vec![PushCell { key: "aaa".to_string(), value: 123.into(), attr: None }]);
        assert_eq!(data_bus.tick(), vec![]);

        data_bus.remote_set("ccc", 456, None);

        assert_eq!(data_bus.get("ccc"), Some(Cell { value: 456.into(), attr: None }));
        assert_eq!(data_bus.tick(), vec![]);

        data_bus.remote_set("ddd", 789, Some(msg!{"aaa": "bbb"}));

        assert_eq!(data_bus.get("ddd"), Some(Cell { value: 789.into(), attr: Some(msg!{"aaa": "bbb"}) }));

        data_bus.remote_set("ddd", 101112, Some(msg!{"aaa": "ccc"}));

        assert_eq!(data_bus.get("ddd"), Some(Cell { value: 101112.into(), attr: Some(msg!{"aaa": "ccc"}) }));
    }

    #[test]
    fn on_set() {
        let data_bus = DataBus::new();

        let data_bus2 = data_bus.clone();
        data_bus.on_set(move |key, value, attr, _| {
            if key == "aaa" {
                data_bus2.set("bbb", value, attr);
            }
        });
    
        data_bus.set("aaa", 123, Some(msg!{"aaa": "bbb"}));

        assert_eq!(data_bus.get("bbb"), Some(Cell { value: 123.into(), attr: Some(msg!{"aaa": "bbb"})}));
    }
}
