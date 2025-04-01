use std::ops::Range;

use object_store::GetRange;

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidGetRange {
    #[error("Wanted range starting at {requested}, but object was only {length} bytes long")]
    StartTooLarge { requested: u64, length: u64 },

    #[error("Range started at {start} and ended at {end}")]
    Inconsistent { start: u64, end: u64 },

    #[error("Range {requested} is larger than system memory limit {max}")]
    TooLarge { requested: u64, max: u64 },
}

pub(crate) trait GetRangeExt {
    fn is_valid(&self) -> Result<(), InvalidGetRange>;

    fn as_range(&self, len: u64) -> Result<Range<u64>, InvalidGetRange>;
}

impl GetRangeExt for GetRange {
    fn is_valid(&self) -> Result<(), InvalidGetRange> {
        if let Self::Bounded(r) = self {
            if r.end <= r.start {
                return Err(InvalidGetRange::Inconsistent {
                    start: r.start,
                    end: r.end,
                });
            }
            if (r.end - r.start) > usize::MAX as u64 {
                return Err(InvalidGetRange::TooLarge {
                    requested: r.start,
                    max: usize::MAX as u64,
                });
            }
        }
        Ok(())
    }

    fn as_range(&self, len: u64) -> Result<Range<u64>, InvalidGetRange> {
        self.is_valid()?;
        match self {
            Self::Bounded(r) => {
                if r.start >= len {
                    Err(InvalidGetRange::StartTooLarge {
                        requested: r.start,
                        length: len,
                    })
                } else if r.end > len {
                    Ok(r.start..len)
                } else {
                    Ok(r.clone())
                }
            }
            Self::Offset(o) => {
                if *o >= len {
                    Err(InvalidGetRange::StartTooLarge {
                        requested: *o,
                        length: len,
                    })
                } else {
                    Ok(*o..len)
                }
            }
            Self::Suffix(n) => Ok(len.saturating_sub(*n)..len),
        }
    }
}
