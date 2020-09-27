// MIT License

// Copyright (c) 2020 Lars Kroll

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// This code is copy pasted from https://github.com/Bathtor/rust-hash-wheel-timer
// Thanks to the author.

use std::fmt::Debug;

/// A single entry in a slot
pub struct Entry<E, R> {
    /// The actual entry
    pub entry: E,
    /// The rest delay associated with the entry
    pub rest: R,
}

/// Just a convenience type alias for the list type used in each slot
pub type EntryList<E, R> = Vec<Entry<E, R>>;

/// A single wheel with 256 slots of for elements of `E`
///
/// The `R` us used to store an array of bytes that are the rest of the delay.
/// This way the same wheel structure can be used at different hierarchical levels.
pub struct ByteWheel<E, R> {
    slots: [Option<EntryList<E, R>>; 256],
    count: u64,
    pos: u8,
}

impl<E, R> ByteWheel<E, R> {
    /// Create a new empty ByteWheel
    pub fn new() -> Self {
        let slots: [Option<EntryList<E, R>>; 256] =
            [
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None, Option::None, Option::None,
            Option::None, Option::None, Option::None, Option::None
            ];

        ByteWheel {
            slots,
            count: 0,
            pos: 0,
        }
    }

    /// Returns the index of the pos slot
    pub fn pos(&self) -> u8 {
        self.pos
    }

    /// Advances the wheel pointer to the target index without executing anything
    ///
    /// Used for implementing "skip"-behaviours.
    pub fn advance(&mut self, to: u8) {
        self.pos = to;
    }

    /// Insert an entry at `pos` into the wheel and store the rest `r` with it
    pub fn insert(&mut self, pos: u8, e: E, r: R) {
        let entry = Entry { entry: e, rest: r };

        let slot = &mut self.slots[pos as usize];

        self.count += 1;

        match slot {
            Some(slot) => {
                slot.push(entry);
            }
            None => {
                *slot = Some(vec![entry]);
            }
        }
    }

    /// True if the number of entries is 0
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Move the wheel by one tick and return all entries in the pos slot together with the index of the next slot
    pub fn tick(&mut self) -> (Option<EntryList<E, R>>, u8) {
        self.pos = self.pos.wrapping_add(1);

        let slot = self.slots[self.pos as usize].take();

        if let Some(ref slot) = slot {
            self.count -= slot.len() as u64;
        }

        (slot, self.pos)
    }
}

impl<E, R> Default for ByteWheel<E, R> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod byte_wheel_tests {
    use super::ByteWheel;

    #[test]
    fn test() {
        let mut wheel = ByteWheel::<usize, ()>::new();
        assert!(wheel.slots.len() == 256);

        let (list, pos) = wheel.tick();
        assert!(list.is_none());
        assert!(pos == 1);
        assert!(wheel.is_empty());

        wheel.insert(pos + 1, 0, ());
        assert!(!wheel.is_empty());

        let (list, pos) = wheel.tick();
        assert!(list.is_some());
        assert!(pos == 2);
        assert!(wheel.is_empty());
    }
}


/// An implementation of four-level byte-sized wheel
pub struct Wheel<E>
where
    E: Debug,
{
    level1: ByteWheel<E, [u8; 0]>,
    level2: ByteWheel<E, [u8; 1]>,
    level3: ByteWheel<E, [u8; 2]>,
    level4: ByteWheel<E, [u8; 3]>,
    pruner: fn(&E) -> bool,
}

const LEVEL1_LEN: u32 = 1 << 8; // 2^8
const LEVEL2_LEN: u32 = 1 << 16; // 2^16
const LEVEL3_LEN: u32 = 1 << 24; // 2^24
const LEVEL4_LEN: u32 = u32::max_value(); // 2^32

pub fn default_prune<E>(_e: &E) -> bool {
    true
}

impl<E> Default for Wheel<E>
where
    E: Debug,
{
    fn default() -> Self {
        Wheel::new(default_prune::<E>)
    }
}

impl<E> Wheel<E>
where
    E: Debug,
{
    /// Create a new wheel
    pub fn new(pruner: fn(&E) -> bool) -> Self {
        Wheel {
            level1: ByteWheel::new(),
            level2: ByteWheel::new(),
            level3: ByteWheel::new(),
            level4: ByteWheel::new(),
            pruner,
        }
    }

    /// Described how many ticks are left before the timer has wrapped around completely
    pub fn remaining(&self) -> u32 {
        LEVEL4_LEN - (self.current() as u32)
    }

    /// Produces a 32-bit timestamp including the current index of every wheel
    pub fn current(&self) -> u32 {
        let bytes = [
            self.level4.pos(),
            self.level3.pos(),
            self.level2.pos(),
            self.level1.pos(),
        ];

        u32::from_be_bytes(bytes)
    }

    /// Insert a new timeout into the wheel to be returned after `delay` ticks
    pub fn insert(&mut self, e: E, delay: u32) -> Result<(), E> {
        let current_time = self.current();

        let absolute_time = delay.wrapping_add(current_time);
        let absolute_bytes = absolute_time.to_be_bytes();

        let zero_time = absolute_time ^ current_time; // a-b%2
        let zero_bytes = zero_time.to_be_bytes();

        match zero_bytes {
            [0, 0, 0, 0] => Err(e),
            [0, 0, 0, _] => {
                self.level1.insert(absolute_bytes[3], e, []);
                Ok(())
            }
            [0, 0, _, _] => {
                self.level2.insert(absolute_bytes[2], e, [absolute_bytes[3]]);
                Ok(())
            }
            [0, _, _, _] => {
                self.level3.insert(
                    absolute_bytes[1],
                    e,
                    [absolute_bytes[2], absolute_bytes[3]],
                );
                Ok(())
            }
            [_, _, _, _] => {
                self.level4.insert(
                    absolute_bytes[0],
                    e,
                    [absolute_bytes[1], absolute_bytes[2], absolute_bytes[3]],
                );
                Ok(())
            }
        }
    }

    /// Move the wheel forward by a single unit
    ///
    /// Returns a list of all timers that expire during this tick.
    pub fn tick(&mut self) -> Vec<E> {
        let mut res: Vec<E> = Vec::new();
        // level1
        let (move0_opt, current0) = self.level1.tick();

        if let Some(move0) = move0_opt {
            res.reserve(move0.len());
            for we in move0 {
                if (self.pruner)(&we.entry) {
                    res.push(we.entry);
                }
            }
        }

        if current0 == 0u8 {
            // level2
            let (move1_opt, current1) = self.level2.tick();

            if let Some(move1) = move1_opt {
                // Don't bother reserving, as most of the values will likely be redistributed over the primary wheel instead of being returned
                for we in move1 {
                    if (self.pruner)(&we.entry) {
                        if we.rest[0] == 0u8 {
                            res.push(we.entry);
                        } else {
                            self.level1.insert(we.rest[0], we.entry, []);
                        }
                    }
                }
            }

            if current1 == 0u8 {
                // level3
                let (move2_opt, current2) = self.level3.tick();

                if let Some(move2) = move2_opt {
                    // Don't bother reserving, as most of the values will likely be redistributed over the primary wheel instead of being returned
                    for we in move2 {
                        if (self.pruner)(&we.entry) {
                            match we.rest {
                                [0, 0] => {
                                    res.push(we.entry);
                                }
                                [0, b0] => {
                                    self.level1.insert(b0, we.entry, []);
                                }
                                [b1, b0] => {
                                    self.level2.insert(b1, we.entry, [b0]);
                                }
                            }
                        }
                    }
                }

                if current2 == 0u8 {
                    // level4
                    let (move3_opt, _) = self.level4.tick();
                    if let Some(move3) = move3_opt {
                        // Don't bother reserving, as most of the values will likely be redistributed over the primary wheel instead of being returned
                        for we in move3 {
                            if (self.pruner)(&we.entry) {
                                match we.rest {
                                    [0, 0, 0] => {
                                        res.push(we.entry);
                                    }
                                    [0, 0, b0] => {
                                        self.level1.insert(b0, we.entry, []);
                                    }
                                    [0, b1, b0] => {
                                        self.level2.insert(b1, we.entry, [b0]);
                                    }
                                    [b2, b1, b0] => {
                                        self.level3.insert(b2, we.entry, [b1, b0]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        res
    }

    /// Skip a certain `amount` of units
    ///
    /// No timers will be executed for the skipped time.
    /// Only use this after determining that it's actually
    /// valid with [can_skip](Wheel::can_skip)!
    pub fn skip(&mut self, amount: u32) {
        let new = self.current().wrapping_add(amount);
        let new_bytes = new.to_be_bytes();

        self.level1.advance(new_bytes[3]);
        self.level2.advance(new_bytes[2]);
        self.level3.advance(new_bytes[1]);
        self.level4.advance(new_bytes[0]);
    }

    /// Determine if and how many ticks can be skipped
    pub fn can_skip(&self) -> Option<u32> {
        if self.level1.is_empty() {
            if self.level2.is_empty() {
                if self.level3.is_empty() {
                    if self.level4.is_empty() {
                        None
                    } else {
                        let tertiary_current = self.current() & (LEVEL3_LEN - 1u32); // just zero highest byte
                        let rem = LEVEL3_LEN - tertiary_current;
                        Some(rem - 1u32)
                    }
                } else {
                    let level2_pos = self.current() & (LEVEL2_LEN - 1u32); // zero highest 2 bytes
                    let rem = LEVEL2_LEN - level2_pos;
                    Some(rem - 1u32)
                }
            } else {
                let level1_pos = self.level1.pos() as u32;
                let rem = LEVEL1_LEN - level1_pos;
                Some(rem - 1u32)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Wheel;

    #[test]
    fn single_schedule_fail() {
        let mut wheel = Wheel::default();
        let id = 1;

        let res = wheel.insert(id, 0);
        assert!(res.is_err());
        assert!(res.err().unwrap() == 1);
    }

    #[test]
    fn single_schedule() {
        let mut wheel = Wheel::default();
        let id = 1;

        let res = wheel.insert(id, 1);
        assert!(res.is_ok());
        assert!(wheel.current() == 0);

        let res = wheel.tick();
        assert!(res.len() == 1);
        assert!(res[0] == id);

        let res = wheel.tick();
        assert!(res.len() == 0);
    }

    #[test]
    fn single_reschedule() {
        let mut wheel = Wheel::default();
        let id = 1;

        wheel.insert(id, 1).unwrap();

        for _ in 0..1000 {
            let res = wheel.tick();
            assert!(res.len() == 1);
            assert!(res[0] == id);

            wheel.insert(id, 1).unwrap();
        }
    }

    #[test]
    fn increasing_schedule() {
        let mut wheel = Wheel::default();
        let mut ids: [usize; 25] = [0; 25];

        for i in 0..25 {
            let delay = 1 << i;
            ids[i] = i;

            wheel.insert(i, delay).unwrap();
        }

        for i in 0..25 {
            let target = 1 << i;
            let prev: u64 = if i == 0 { 0 } else { 1 << (i - 1) };

            for _ in (prev + 1)..target {
                let res = wheel.tick();
                assert!(res.len() == 0);
            }

            let res = wheel.tick();
            assert!(res.len() == 1);
            assert!(res[0] == ids[i]);
        }
    }

    #[test]
    fn increasing_skip() {
        let mut wheel = Wheel::default();
        let mut ids: [usize; 25] = [0; 25];
        let mut delays: [u32; 25] = [0; 25];

        for i in 0..25 {
            let delay = 1 << i;
            ids[i] = i;
            delays[i] = delay;
            wheel.insert(i, delay).unwrap();
        }

        let mut index = 0;
        let mut ticks = 0u128;

        while index < 25 {
            let res = wheel.tick();
            ticks += 1;
            if res.is_empty() {
                if let Some(skip) = wheel.can_skip() {
                    wheel.skip(skip);
                    ticks += skip as u128;
                }
            } else {
                assert!(res[0] == ids[index]);
                assert!(ticks == delays[index].into());

                index += 1;
            }
        }

        assert!(wheel.can_skip().is_none());
    }
}
