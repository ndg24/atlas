//! Minimal HyperLogLog (Flajolet et al.) cardinality estimator used to
//! populate `Statistics.distinct_count_estimate` while writing a column's
//! stats. Fixed at 2^12 (4096) registers — a standard error of roughly
//! 1.04/sqrt(4096) ~= 1.6%, small enough to be a useful estimate while
//! keeping the per-column sketch cheap to build during a normal write pass.
//!
//! Each file's `Statistics` stores only the final estimate (not the sketch
//! itself), so combining estimates across a dataset's manifests is done by
//! summing per-file numbers (capped at total row count) rather than merging
//! sketches — an approximation, not an exact union.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const PRECISION: u32 = 12;
const NUM_REGISTERS: usize = 1 << PRECISION;

pub struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; NUM_REGISTERS],
        }
    }

    pub fn insert(&mut self, value: &[u8]) {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();

        let idx = (hash as usize) & (NUM_REGISTERS - 1);
        let rest = hash >> PRECISION;
        // rest's top PRECISION bits are always zero (they were shifted out of a
        // 64-bit hash), so leading_zeros() >= PRECISION always holds.
        let rank = (rest.leading_zeros() - PRECISION + 1) as u8;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    pub fn estimate(&self) -> u64 {
        let m = NUM_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw = alpha * m * m / sum;

        let estimate = if raw <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros != 0 {
                m * (m / zeros as f64).ln()
            } else {
                raw
            }
        } else {
            raw
        };

        estimate.round().max(0.0) as u64
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_high_cardinality_within_tolerance() {
        let mut hll = HyperLogLog::new();
        for i in 0..100_000i64 {
            hll.insert(&i.to_le_bytes());
        }
        let estimate = hll.estimate();
        let error = (estimate as f64 - 100_000.0).abs() / 100_000.0;
        assert!(error < 0.1, "estimate {estimate} too far from 100000");
    }

    #[test]
    fn estimates_low_cardinality_with_repeats() {
        let mut hll = HyperLogLog::new();
        for i in 0..50i64 {
            for _ in 0..20 {
                hll.insert(&i.to_le_bytes());
            }
        }
        let estimate = hll.estimate();
        assert!(
            (30..=80).contains(&estimate),
            "estimate {estimate} too far from true distinct count 50"
        );
    }

    #[test]
    fn empty_sketch_estimates_zero() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.estimate(), 0);
    }
}
