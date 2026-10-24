use crate::messages::{Instance, Position, Sequence};

pub type GlobalSn = u64;

/// A finite, contiguous portion of the global log. Global SNs start at one.
#[derive(Clone, Copy, Debug)]
pub struct Epoch {
    pub number: u64,
    length: u64,
    segments: u64,
    first_sn: GlobalSn,
    last_sn: GlobalSn,
}

impl Epoch {
    pub fn new(number: u64, length: u64, segments: u64) -> Option<Self> {
        if length == 0 || segments == 0 {
            return None;
        }
        let base = number.checked_mul(length)?;
        Some(Self {
            number,
            length,
            segments,
            first_sn: base.checked_add(1)?,
            last_sn: base.checked_add(length)?,
        })
    }

    pub fn first_sn(&self) -> GlobalSn {
        self.first_sn
    }

    pub fn last_sn(&self) -> GlobalSn {
        self.last_sn
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn segments(&self) -> u64 {
        self.segments
    }

    pub fn next(&self) -> Option<Self> {
        Self::new(self.number.checked_add(1)?, self.length, self.segments)
    }

    pub fn segment(&self, instance: Instance) -> Option<Segment> {
        if instance >= self.segments {
            return None;
        }
        let slots = if instance < self.length {
            1 + (self.length - 1 - instance) / self.segments
        } else {
            0
        };
        Some(Segment {
            epoch: self.number,
            instance,
            slots,
            first_sn: self.first_sn,
            stride: self.segments,
        })
    }

    pub fn global_sn(&self, position: Position) -> Option<GlobalSn> {
        if position.epoch != self.number {
            return None;
        }
        self.segment(position.instance)?
            .global_sn(position.sequence)
    }

    pub fn position(&self, sn: GlobalSn) -> Option<Position> {
        if sn < self.first_sn || sn > self.last_sn {
            return None;
        }
        let offset = sn - self.first_sn;
        Some(Position::for_epoch(
            self.number,
            offset % self.segments,
            offset / self.segments + 1,
        ))
    }
}

/// One epoch's round-robin subset of global SNs, with a finite local sequence.
#[derive(Clone, Copy, Debug)]
pub struct Segment {
    pub epoch: u64,
    pub instance: Instance,
    pub slots: Sequence,
    first_sn: GlobalSn,
    stride: u64,
}

impl Segment {
    pub fn global_sn(&self, sequence: Sequence) -> Option<GlobalSn> {
        if sequence == 0 || sequence > self.slots {
            return None;
        }
        self.first_sn
            .checked_add((sequence - 1).checked_mul(self.stride)?)?
            .checked_add(self.instance)
    }
}

#[cfg(test)]
#[path = "tests/epoch_tests.rs"]
mod tests;
