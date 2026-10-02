use super::workload::{Op, OPS};

/// Log-linear latency histogram (4 sub-buckets per power of two, ~12% resolution), in ns.
#[derive(Clone)]
pub struct Histogram {
    buckets: Vec<u64>,
    count: u64,
}

impl Histogram {
    pub fn new() -> Self {
        Self { buckets: vec![0; 64 * 4], count: 0 }
    }

    #[inline(always)]
    pub fn record(&mut self, ns: u64) {
        let idx = if ns < 4 {
            ns as usize
        } else {
            let e = 63 - ns.leading_zeros() as usize;
            e * 4 + ((ns >> (e - 2)) & 3) as usize
        };
        self.buckets[idx] += 1;
        self.count += 1;
    }

    pub fn merge(&mut self, other: &Histogram) {
        self.buckets.iter_mut().zip(&other.buckets).for_each(|(a, b)| *a += b);
        self.count += other.count;
    }

    /// Lower bound of the bucket holding the `q`-quantile.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0
        }
        let target = ((self.count as f64 * q).ceil() as u64).max(1);
        let mut seen = 0;
        for (idx, c) in self.buckets.iter().enumerate() {
            seen += c;
            if seen >= target {
                return if idx < 4 { idx as u64 } else { (4 + (idx % 4) as u64) << (idx / 4 - 2) }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct OpStats {
    pub attempted: u64,
    pub succeeded: u64,
    pub latency: Histogram,
}

/// Per-thread statistics, merged after the run.
#[derive(Clone)]
pub struct ThreadStats {
    pub ops: Vec<OpStats>,
    /// Reads/scans that returned data violating the workload's invariant (payload == key,
    /// ascending keys within the range), i.e., a corrupted snapshot.
    pub violations: u64,
    pub scanned_records: u64,
}

impl ThreadStats {
    pub fn new() -> Self {
        Self {
            ops: OPS.iter().map(|_| OpStats { attempted: 0, succeeded: 0, latency: Histogram::new() }).collect(),
            violations: 0,
            scanned_records: 0,
        }
    }

    #[inline(always)]
    pub fn record(&mut self, op: Op, ok: bool, ns: u64) {
        let s = &mut self.ops[op.index()];
        s.attempted += 1;
        s.succeeded += ok as u64;
        s.latency.record(ns);
    }

    pub fn merge(&mut self, other: &ThreadStats) {
        for (a, b) in self.ops.iter_mut().zip(&other.ops) {
            a.attempted += b.attempted;
            a.succeeded += b.succeeded;
            a.latency.merge(&b.latency);
        }
        self.violations += other.violations;
        self.scanned_records += other.scanned_records;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_monotone_and_close() {
        let mut h = Histogram::new();
        (1..=10_000u64).for_each(|v| h.record(v * 100));
        let (p50, p99) = (h.quantile(0.5), h.quantile(0.99));
        assert!(p50 <= p99);
        assert!((p50 as f64 - 500_000.0).abs() / 500_000.0 < 0.25, "p50 = {p50}");
        assert!((p99 as f64 - 990_000.0).abs() / 990_000.0 < 0.25, "p99 = {p99}");
    }
}
