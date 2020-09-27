use std::time::Instant;

#[derive(Debug, Clone)]
pub struct KeepAlive {
    pub idle: u32,
    pub interval: u32,
    pub count: u32,
    time: Instant,
    detect: bool,
    counter: u32
}

impl Default for KeepAlive {
    fn default() -> Self {
        Self {
            idle: 60,
            interval: 10,
            count: 3,
            time: Instant::now(),
            detect: false,
            counter: 3
        }
    }
}

impl KeepAlive {
    pub fn new(idle: u32, interval: u32, count: u32) -> Self {
        Self {
            idle,
            interval,
            count,
            time: Instant::now(),
            detect: false,
            counter: count
        }
    }

    pub fn reset(&mut self, now: Instant) {
        self.time = now;
        self.detect = false;
        self.counter = self.count;
    }

    pub fn tick(&mut self, now: Instant) -> Option<(u32, bool)> {
        let since = now.duration_since(self.time).as_secs() as u32;

        if self.detect {
            if self.counter == 0 {
                None
            } else if since < self.interval {
                Some((self.interval - since, false))
            } else {
                self.counter = self.counter.saturating_sub(1);
                Some((self.interval, true))
            }
        } else if since < self.idle {
            Some((self.idle - since, false))
        } else {
            self.detect = true;
            self.counter = self.counter.saturating_sub(1);
            Some((self.interval, true))
        }
    }
}
