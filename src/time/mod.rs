use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::VecDeque;

pub struct Wheel<T> {
    wheel: Vec<Vec<Task<T>>>,
    tick_ms: usize,
    tick_num: usize,
    cur_index: usize,
    cur_time: Duration
}

#[derive(Clone, Debug)]
pub struct Task<T> {
    pub data: T,
    pub time: Duration,
    pub id: Option<String>
}

impl<T: Clone> Wheel<T> {
    pub fn new(tick_ms: usize, tick_num: usize) -> Wheel<T> {
        Wheel {
            wheel: vec![vec![]; tick_num],
            tick_ms,
            tick_num,
            cur_index: 0,
            cur_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        }
    }

    pub fn push(&mut self, mut task: Task<T>) {
        if task.time.as_secs() > 2 {
            return
        }

        let mut index = task.time.as_millis() as usize / self.tick_ms;

        if index + self.cur_index > self.tick_num - 1 {
            index = index + self.cur_index - self.tick_num - 1;
        } else {
            index += self.cur_index;
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        task.time += now;

        self.wheel[index].push(task);

        if (now - self.cur_time).as_millis() > 50 {

        }
    }
}
