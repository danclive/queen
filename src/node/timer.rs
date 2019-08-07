use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use queen_io::sys::timerfd::{TimerFd, TimerSpec};

#[derive(Clone, Debug)]
pub struct Task<T> {
    pub data: T,
    pub time: Duration,
    pub id: Option<String>
}

impl<T> PartialOrd for Task<T> {
    fn partial_cmp(&self, other: &Task<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Task<T> {
    fn cmp(&self, other: &Task<T>) -> Ordering {
        match self.time.cmp(&other.time) {
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Less,
            Ordering::Less => Ordering::Greater
        }
    }
}

impl<T> PartialEq for Task<T> {
    fn eq(&self, other: &Task<T>) -> bool {
        self.time == other.time
    }
}

impl<T> Eq for Task<T> {}

pub struct Timer<T> {
    tasks: BinaryHeap<Task<T>>,
    timerfd: TimerFd
}

impl<T: Clone> Timer<T> {
    pub fn new() -> io::Result<Timer<T>> {
        Ok(Timer {
            tasks: BinaryHeap::new(),
            timerfd: TimerFd::new()?
        })
    }

    pub fn peek(&self) -> Option<&Task<T>> {
        self.tasks.peek()
    }

    pub fn push(&mut self, task: Task<T>) -> io::Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let mut task = task;
        task.time += now;

        if let Some(peek_task) = self.peek() {
            if task > *peek_task {
                self.settime(true, TimerSpec {
                    interval: Duration::new(0, 0),
                    value: task.time
                })?;
            }
        } else {
            self.settime(true, TimerSpec {
                interval: Duration::new(0, 0),
                value: task.time
            })?;
        }

        self.tasks.push(task);

        Ok(())
    }

    #[inline]
    pub fn pop(&mut self) -> Option<Task<T>> {
        self.tasks.pop()
    }

    #[inline]
    pub fn settime(&self, abstime: bool, value: TimerSpec) -> io::Result<TimerSpec>{
        self.timerfd.settime(abstime, value)
    }

    #[inline]
    pub fn gettime(&self) -> io::Result<TimerSpec> {
        self.timerfd.gettime()
    }

    #[inline]
    pub fn done(&self) -> io::Result<u64> {
        self.timerfd.read()
    }

    pub fn remove(&mut self, id: String) -> bool {
        let id = Some(id);

        let mut tasks_vec: Vec<Task<T>> = Vec::from(self.tasks.clone());

        let mut has = false;

        if let Some(pos) = tasks_vec.iter().position(|x| x.id == id) {
            tasks_vec.remove(pos);
            has  = true;
        }

        self.tasks = tasks_vec.into();

        has
    }

    pub fn refresh(&mut self) -> io::Result<()> {
        if let Some(task) = self.peek() {
            self.settime(true, TimerSpec {
                interval: Duration::new(0, 0),
                value: task.time
            })?;
        }

        Ok(())
    }
}

impl<T> AsRawFd for Timer<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.timerfd.as_raw_fd()
    }
}
