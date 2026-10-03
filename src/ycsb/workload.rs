//! YCSB-style workload model: per-operation key sampling (uniform / zipfian / latest / hotspot)
//! over a dense keyspace, operation mixes, and a tiny deterministic RNG. Operations are
//! generated online by the worker threads, so there is no pre-generated trace that has to be
//! replayed in a serial order.

use std::sync::Arc;

pub type Key = u64;

/// SplitMix64-seeded xoshiro256**: fast, deterministic per-thread streams.
#[derive(Clone)]
pub struct Rng([u64; 4]);

impl Rng {
    pub fn new(seed: u64) -> Self {
        let mut z = seed;
        let mut next = || {
            z = z.wrapping_add(0x9E3779B97F4A7C15);
            let mut x = z;
            x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
            x ^ (x >> 31)
        };
        Rng([next(), next(), next(), next()])
    }

    #[inline(always)]
    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.0;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    /// Uniform in [0, 1).
    #[inline(always)]
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform in [0, n).
    #[inline(always)]
    pub fn below(&mut self, n: u64) -> u64 {
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Dist {
    Uniform,
    /// YCSB zipfian over ranks, rank 0 hottest.
    Zipfian(f64),
    /// Zipfian over the distance from the most recently inserted key.
    Latest(f64),
    /// `hot_frac` of the keys receive `hot_prob` of the operations.
    Hotspot { hot_frac: f64, hot_prob: f64 },
}

impl Dist {
    pub fn parse(name: &str, theta: f64, hot_frac: f64, hot_prob: f64) -> Result<Dist, String> {
        if !theta.is_finite() || theta < 0.0 {
            return Err("--theta must be a finite number >= 0".into());
        }
        if !hot_frac.is_finite() || !(0.0..=1.0).contains(&hot_frac) {
            return Err("--hot-frac must be a finite number in [0, 1]".into());
        }
        if !hot_prob.is_finite() || !(0.0..=1.0).contains(&hot_prob) {
            return Err("--hot-prob must be a finite number in [0, 1]".into());
        }
        match name {
            "uniform" => Ok(Dist::Uniform),
            "zipf" | "zipfian" if theta <= 0.0 => Ok(Dist::Uniform),
            "zipf" | "zipfian" => Ok(Dist::Zipfian(theta)),
            "latest" => Ok(Dist::Latest(theta)),
            "hotspot" => Ok(Dist::Hotspot { hot_frac, hot_prob }),
            other => Err(format!("unknown distribution '{other}' (uniform|zipf|latest|hotspot)")),
        }
    }
}

/// Zipfian distribution over `n` ranks, P(rank k) ~ 1 / k^theta for any theta > 0 (also theta = 1 and theta > 1, which
/// the generator YCSB uses, Gray et al.'s, does not support). Rejection-inversion sampling after Hörmann and
/// Derflinger (as in Apache Commons Math): exact, O(1) set-up and O(1) expected time per sample.
pub struct ZipfParams {
    n: u64,
    theta: f64,
    h_integral_x1: f64,
    h_integral_n: f64,
    s: f64,
}

impl ZipfParams {
    pub fn new(n: u64, theta: f64) -> Self {
        assert!(theta > 0.0 && n >= 1, "zipfian needs theta > 0 and at least one rank");
        let mut z = Self { n, theta, h_integral_x1: 0.0, h_integral_n: 0.0, s: 0.0 };
        z.h_integral_x1 = z.h_integral(1.5) - 1.0;
        z.h_integral_n = z.h_integral(n as f64 + 0.5);
        z.s = 2.0 - z.h_integral_inverse(z.h_integral(2.5) - z.h(2.0));
        z
    }

    fn h(&self, x: f64) -> f64 {
        (-self.theta * x.ln()).exp()
    }

    fn h_integral(&self, x: f64) -> f64 {
        let log_x = x.ln();
        helper2((1.0 - self.theta) * log_x) * log_x
    }

    fn h_integral_inverse(&self, x: f64) -> f64 {
        let t = (x * (1.0 - self.theta)).max(-1.0);
        (helper1(t) * x).exp()
    }

    /// A rank in [0, n), 0 being the hottest.
    #[inline]
    pub fn sample(&self, rng: &mut Rng) -> u64 {
        loop {
            let u = self.h_integral_n + rng.next_f64() * (self.h_integral_x1 - self.h_integral_n);
            let x = self.h_integral_inverse(u);
            let k = ((x + 0.5) as u64).clamp(1, self.n);
            if k as f64 - x <= self.s || u >= self.h_integral(k as f64 + 0.5) - self.h(k as f64) {
                return k - 1
            }
        }
    }
}

/// (exp(x) - 1) / x, stable around 0.
fn helper2(x: f64) -> f64 {
    if x.abs() > 1e-8 { x.exp_m1() / x } else { 1.0 + x * 0.5 * (1.0 + x / 3.0 * (1.0 + 0.25 * x)) }
}

/// ln(1 + x) / x, stable around 0.
fn helper1(x: f64) -> f64 {
    if x.abs() > 1e-8 { x.ln_1p() / x } else { 1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x)) }
}

/// Maps ranks to ids bijectively and pseudo-randomly, so hot keys spread over the whole key
/// range instead of clustering in the first leaves (YCSB's "scrambled zipfian").
#[derive(Clone, Copy)]
struct Scramble {
    n: u64,
    a: u64,
    b: u64,
}

impl Scramble {
    fn new(n: u64) -> Self {
        fn gcd(a: u64, b: u64) -> u64 { if b == 0 { a } else { gcd(b, a % b) } }
        let mut a = ((n as f64 * 0.6180339887) as u64) | 1;
        while gcd(a, n) != 1 { a += 2 }
        Self { n, a, b: n / 3 }
    }

    #[inline(always)]
    fn apply(&self, rank: u64) -> u64 {
        ((rank as u128 * self.a as u128 + self.b as u128) % self.n as u128) as u64
    }
}

/// Chooses existing keys. Keys are dense ids `0..records`, so a scan over `[k, k + len)`
/// touches `len` consecutive records.
pub struct KeyChooser {
    dist: Dist,
    records: u64,
    zipf: Option<Arc<ZipfParams>>,
    scramble: Option<Scramble>,
}

impl KeyChooser {
    pub fn new(dist: Dist, records: u64, scramble: bool) -> Self {
        let zipf = match dist {
            Dist::Zipfian(theta) | Dist::Latest(theta) => Some(Arc::new(ZipfParams::new(records, theta))),
            _ => None,
        };
        let scramble = (scramble && matches!(dist, Dist::Zipfian(..) | Dist::Hotspot { .. }))
            .then(|| Scramble::new(records));
        Self { dist, records, zipf, scramble }
    }

    pub fn share(&self) -> Self {
        Self {
            dist: self.dist,
            records: self.records,
            zipf: self.zipf.clone(),
            scramble: self.scramble,
        }
    }

    /// Chooses a key in the live window `[lo, hi]`. The window is `[0, records)` for static
    /// workloads; with fresh inserts and expiring deletes it slides, so ranks are folded onto
    /// the current window.
    #[inline]
    pub fn next(&self, rng: &mut Rng, lo: Key, hi: Key) -> Key {
        let window = hi.saturating_sub(lo) + 1;
        let rank = match self.dist {
            Dist::Uniform => return lo + rng.below(window),
            Dist::Zipfian(..) => self.zipf.as_ref().unwrap().sample(rng),
            Dist::Latest(..) => {
                let back = self.zipf.as_ref().unwrap().sample(rng);
                return hi - back.min(window - 1)
            }
            Dist::Hotspot { hot_frac, hot_prob } => {
                let hot = ((self.records as f64 * hot_frac) as u64).max(1);
                if rng.next_f64() < hot_prob {
                    rng.below(hot)
                } else {
                    hot + rng.below((self.records - hot).max(1))
                }
            }
        };

        let id = match self.scramble {
            Some(s) => s.apply(rank),
            None => rank,
        };
        lo + id % window
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    Read,
    Update,
    Insert,
    Delete,
    Scan,
    ReadModifyWrite,
}

pub const OPS: [Op; 6] = [Op::Read, Op::Update, Op::Insert, Op::Delete, Op::Scan, Op::ReadModifyWrite];

impl Op {
    pub const fn name(self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Update => "update",
            Op::Insert => "insert",
            Op::Delete => "delete",
            Op::Scan => "scan",
            Op::ReadModifyWrite => "rmw",
        }
    }

    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Operation mix in percent, order of `OPS`.
#[derive(Clone, Copy, Debug)]
pub struct Mix(pub [f64; 6]);

impl Mix {
    pub fn parse(spec: &str) -> Result<Mix, String> {
        let parts = spec.split(':')
            .map(|p| p.parse::<f64>().map_err(|_| format!("bad mix component '{p}'")))
            .collect::<Result<Vec<_>, _>>()?;

        if parts.len() != 6 {
            return Err("--mix needs read:update:insert:delete:scan:rmw (percent)".into());
        }

        let mut mix = [0.0; 6];
        mix.copy_from_slice(&parts);
        if mix.iter().any(|share| !share.is_finite() || *share < 0.0) {
            return Err("--mix components must be finite and non-negative".into());
        }
        if (mix.iter().sum::<f64>() - 100.0).abs() > 1e-6 {
            return Err("--mix must sum to 100".into());
        }

        Ok(Mix(mix))
    }

    #[inline]
    pub fn pick(&self, rng: &mut Rng) -> Op {
        let mut x = rng.next_f64() * 100.0;
        for (op, share) in OPS.iter().zip(self.0.iter()) {
            if x < *share {
                return *op
            }
            x -= share;
        }
        Op::Read
    }
}

/// How deletes choose their key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeleteKeys {
    /// Drawn from the key distribution. Deleted keys stay deleted (inserts always use fresh
    /// ids), so the success rate decays over time.
    FromDistribution,
    /// Strictly the oldest live key (FIFO expiry): together with fresh inserts the live set is a
    /// sliding window of constant size, a steady state with continuous reorganization and dead
    /// data (deletions are where the list-based schemes degrade most).
    Oldest,
}

pub struct Preset {
    pub mix: Mix,
    pub dist: Dist,
    pub deletes: DeleteKeys,
}

/// Mix order: read, update, insert, delete, scan, rmw.
pub fn preset(name: &str, theta: f64) -> Option<Preset> {
    let zipf = if theta <= 0.0 { Dist::Uniform } else { Dist::Zipfian(theta) };
    let p = |mix, dist, deletes| Some(Preset { mix: Mix(mix), dist, deletes });
    match name {
        "a" => p([50., 50., 0., 0., 0., 0.], zipf, DeleteKeys::FromDistribution),
        "b" => p([95., 5., 0., 0., 0., 0.], zipf, DeleteKeys::FromDistribution),
        "c" => p([100., 0., 0., 0., 0., 0.], zipf, DeleteKeys::FromDistribution),
        "d" => p([95., 0., 5., 0., 0., 0.], Dist::Latest(if theta <= 0.0 { 0.99 } else { theta }), DeleteKeys::FromDistribution),
        "e" => p([0., 0., 5., 0., 95., 0.], zipf, DeleteKeys::FromDistribution),
        "f" => p([50., 0., 0., 0., 0., 50.], zipf, DeleteKeys::FromDistribution),
        // Not part of YCSB: sliding window with fresh inserts and FIFO expiry (see `DeleteKeys::Oldest`).
        "churn" => p([25., 25., 25., 25., 0., 0.], zipf, DeleteKeys::Oldest),
        "update-heavy" => p([10., 90., 0., 0., 0., 0.], zipf, DeleteKeys::FromDistribution),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scramble_is_a_bijection() {
        for n in [1u64, 2, 10, 1000, 4096, 9973] {
            let s = Scramble::new(n);
            let mut seen = vec![false; n as usize];
            (0..n).for_each(|r| {
                let id = s.apply(r) as usize;
                assert!(!seen[id]);
                seen[id] = true;
            });
        }
    }

    /// The sampler must reproduce P(k) = k^-theta / H for every theta, in particular above 1 and at 1.
    #[test]
    fn zipfian_matches_its_probability_mass_function() {
        let n = 50u64;
        for theta in [0.2, 0.5, 0.8, 0.99, 1.0, 1.2, 1.4, 2.0] {
            let z = ZipfParams::new(n, theta);
            let mut rng = Rng::new(11);
            let samples = 400_000;
            let mut counts = vec![0u64; n as usize];
            (0..samples).for_each(|_| {
                let r = z.sample(&mut rng);
                assert!(r < n);
                counts[r as usize] += 1;
            });
            let norm: f64 = (1..=n).map(|k| (k as f64).powf(-theta)).sum();
            for k in [1u64, 2, 3, 5, 10, 25, 50] {
                let expected = (k as f64).powf(-theta) / norm;
                let got = counts[(k - 1) as usize] as f64 / samples as f64;
                let tolerance = 5.0 * (expected * (1.0 - expected) / samples as f64).sqrt() + 1e-4;
                assert!((got - expected).abs() < tolerance, "theta {theta}, rank {k}: {got:.5} vs {expected:.5}");
            }
        }
    }

    #[test]
    fn zipfian_handles_huge_domains_and_a_single_rank() {
        let mut rng = Rng::new(2);
        let big = ZipfParams::new(1 << 40, 0.99);
        assert!((0..10_000).all(|_| big.sample(&mut rng) < 1 << 40));
        let one = ZipfParams::new(1, 1.4);
        assert!((0..100).all(|_| one.sample(&mut rng) == 0));
    }

    #[test]
    fn mix_follows_shares() {
        let mix = Mix::parse("50:25:25:0:0:0").unwrap();
        let mut rng = Rng::new(1);
        let mut c = [0usize; 6];
        (0..100_000).for_each(|_| c[mix.pick(&mut rng).index()] += 1);
        assert!(c[0] > 48_000 && c[0] < 52_000);
        assert_eq!(c[3] + c[4] + c[5], 0);
    }
}
