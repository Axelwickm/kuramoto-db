//! Minimal, dependency-light Rateless-IBLT (RIBLT) in pure Rust.
//!
//! * Encoder streams an infinite sequence of coded symbols.
//! * Decoder peels differences between local and remote sets.

use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;

// ---------- 1.  Symbol abstraction -----------------------------------------------------------

/// Trait for a reconciliable symbol.
/// Implemented here for `u64` but can be implemented for fixed-size byte blobs.
pub trait Symbol: Copy + Eq + Default + Debug {
    fn xor(self, other: Self) -> Self;
    fn hash(self) -> u64;
}

impl Symbol for u64 {
    #[inline]
    fn xor(self, other: Self) -> Self {
        self ^ other
    }
    #[inline]
    fn hash(self) -> u64 {
        // Tiny integer hash (splitmix) – deterministic & good enough here.
        let mut z = self.wrapping_add(0x9E3779B97F4A7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

// ---------- 2.  Primitive structs ------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct HashedSymbol<S: Symbol> {
    sym: S,
    hash: u64,
}
impl<S: Symbol> From<S> for HashedSymbol<S> {
    fn from(sym: S) -> Self {
        Self {
            hash: sym.hash(),
            sym,
        }
    }
}

#[derive(Clone, Copy)]
pub struct CodedSymbol<S: Symbol> {
    count: i64,
    hash: u64,
    symxor: S,
}
impl<S: Symbol> Default for CodedSymbol<S> {
    fn default() -> Self {
        Self {
            count: 0,
            hash: 0,
            symxor: S::default(),
        }
    }
}
impl<S: Symbol> CodedSymbol<S> {
    #[inline]
    fn apply(mut self, hs: HashedSymbol<S>, dir: i64) -> Self {
        self.count += dir;
        self.hash ^= hs.hash;
        self.symxor = self.symxor.xor(hs.sym);
        self
    }
    #[inline]
    fn decodable(&self) -> bool {
        matches!(self.count, -1 | 0 | 1)
            && ((self.count == 0 && self.hash == 0) || (self.hash == self.symxor.hash()))
    }
}

// ---------- 3.  Per-item mapping generator  --------------------------------------------------
#[derive(Clone, Copy)]
struct RandMap {
    seed: u64,
    last: u64,
}

impl RandMap {
    fn new(seed: u64) -> Self {
        Self { seed, last: 0 }
    }

    /// Advance by a *small, random* gap (1‥=16) so singletons appear quickly.
    fn next(&mut self) -> u64 {
        // xorshift64*  (Marsaglia, 2003)
        self.seed ^= self.seed >> 12;
        self.seed ^= self.seed << 25;
        self.seed ^= self.seed >> 27;
        let rand = self.seed.wrapping_mul(0x2545F4914F6CDD1D);

        // The Go version uses a more complex gap calculation
        // Let's match it more closely
        let gap = (rand % 16) + 1; // 1..=16
        self.last = self.last.wrapping_add(gap);
        self.last
    }
}

// ---------- 4.  codingWindow – core queue logic  ---------------------------------------------

#[derive(Clone, Copy)]
struct MapEntry {
    src_idx: usize,
    next_coded: u64,
}

impl Ord for MapEntry {
    fn cmp(&self, o: &Self) -> Ordering {
        o.next_coded.cmp(&self.next_coded)
    } // min-heap
}
impl PartialOrd for MapEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl PartialEq for MapEntry {
    fn eq(&self, o: &Self) -> bool {
        self.next_coded == o.next_coded
    }
}
impl Eq for MapEntry {}

#[derive(Clone, Default)]
pub struct CodingWindow<S: Symbol> {
    syms: Vec<HashedSymbol<S>>,
    maps: Vec<RandMap>,
    queue: BinaryHeap<MapEntry>,
    next_i: u64,
}

impl<S: Symbol> CodingWindow<S> {
    fn add(&mut self, hs: HashedSymbol<S>) {
        let idx = self.syms.len();
        let map = RandMap::new(hs.hash);
        self.syms.push(hs);
        self.maps.push(map);
        self.queue.push(MapEntry {
            src_idx: idx,
            next_coded: map.last, // Initialize with current last value (0)
        });
    }

    fn add_with_map(&mut self, hs: HashedSymbol<S>, m: RandMap) {
        let idx = self.syms.len();
        self.syms.push(hs);
        self.maps.push(m);
        self.queue.push(MapEntry {
            src_idx: idx,
            next_coded: m.last, // Use the mapping's current last value
        });
    }

    fn apply(&mut self, mut cell: CodedSymbol<S>, dir: i64) -> CodedSymbol<S> {
        while let Some(top) = self.queue.peek() {
            if top.next_coded != self.next_i {
                break;
            }
            let mut top = self.queue.pop().unwrap();
            let hs = self.syms[top.src_idx];
            cell = cell.apply(hs, dir);

            // Generate next mapping index
            let next = self.maps[top.src_idx].next();
            top.next_coded = next;
            self.queue.push(top);
        }
        self.next_i += 1;
        cell
    }

    fn reset(&mut self) {
        self.syms.clear();
        self.maps.clear();
        self.queue.clear();
        self.next_i = 0;
    }
}

// ---------- 5.  Encoder ----------------------------------------------------------------------

pub struct Encoder<S: Symbol> {
    cw: CodingWindow<S>,
}
impl<S: Symbol> Encoder<S> {
    pub fn new() -> Self {
        Self {
            cw: CodingWindow::default(),
        }
    }
    pub fn add_symbol(&mut self, s: S) {
        self.cw.add(s.into())
    }
    pub fn produce_next(&mut self) -> CodedSymbol<S> {
        self.cw.apply(CodedSymbol::default(), 1)
    }
}

// ---------- 6.  Decoder ----------------------------------------------------------------------

pub struct Decoder<S: Symbol> {
    coded: Vec<CodedSymbol<S>>,
    win: CodingWindow<S>,    // initial local set B
    local: CodingWindow<S>,  // extras only-local
    remote: CodingWindow<S>, // extras only-remote
    decodable: Vec<usize>,
    done: usize,
}
impl<S: Symbol> Decoder<S> {
    pub fn new() -> Self {
        Self {
            coded: Vec::new(),
            win: CodingWindow::default(),
            local: CodingWindow::default(),
            remote: CodingWindow::default(),
            decodable: Vec::new(),
            done: 0,
        }
    }
    pub fn add_symbol(&mut self, s: S) {
        self.win.add(s.into())
    }

    pub fn add_coded_symbol(&mut self, mut c: CodedSymbol<S>) {
        c = self.win.apply(c, -1);
        c = self.remote.apply(c, -1);
        c = self.local.apply(c, 1);

        self.coded.push(c);
        let idx = self.coded.len() - 1;

        // Check if decodable - only add to decodable list if count is 1 or -1
        // (not 0, as those will be handled when they transition from 1/-1 to 0)
        if matches!(c.count, -1 | 1) && c.decodable() {
            self.decodable.push(idx);
        } else if c.count == 0 && c.hash == 0 {
            self.decodable.push(idx);
        }

        // println!(
        //     "+coded idx={:?} count={:?} hash={:?} symxor={:?}",
        //     idx, c.count, c.hash, c.symxor
        // );
    }

    fn apply_new_symbol(&mut self, hs: HashedSymbol<S>, dir: i64) -> RandMap {
        let mut m = RandMap::new(hs.hash);

        // println!(
        //     "→ peeling sym {:?} dir={} starting len={} m.last=0",
        //     hs.sym,
        //     dir,
        //     self.coded.len()
        // );

        while (m.last as usize) < self.coded.len() {
            let cidx = m.last as usize;
            self.coded[cidx] = self.coded[cidx].apply(hs, dir);

            // Check if newly decodable - only add degree 1 or -1 symbols
            if matches!(self.coded[cidx].count, -1 | 1) && self.coded[cidx].decodable() {
                // println!(
                //     "   decodable now idx={} count={} hash={}",
                //     cidx, self.coded[cidx].count, self.coded[cidx].hash
                // );
                self.decodable.push(cidx);
            }

            m.next();
        }
        m
    }

    pub fn try_decode(&mut self) {
        while let Some(idx) = self.decodable.pop() {
            let c = self.coded[idx];

            // Verify it's still decodable (might have changed due to peeling)
            if !c.decodable() {
                continue;
            }

            match c.count {
                1 => {
                    // Remote-only symbol
                    let hs = HashedSymbol {
                        sym: c.symxor,
                        hash: c.hash,
                    };
                    let m = self.apply_new_symbol(hs, -1);
                    self.remote.add_with_map(hs, m);
                    self.done += 1;
                }
                -1 => {
                    // Local-only symbol
                    let hs = HashedSymbol {
                        sym: c.symxor,
                        hash: c.hash,
                    };
                    let m = self.apply_new_symbol(hs, 1);
                    self.local.add_with_map(hs, m);
                    self.done += 1;
                }
                0 => {
                    // Empty symbol
                    self.done += 1;
                }
                _ => {
                    // Should not happen for decodable symbols
                    continue;
                }
            }
        }

        // println!(
        //     "→ done={}/{} rem={:?} loc={:?}",
        //     self.done,
        //     self.coded.len(),
        //     self.remote_diff(),
        //     self.local_diff()
        //);
    }

    pub fn is_decoded(&self) -> bool {
        self.done == self.coded.len()
    }

    pub fn remote_diff(&self) -> Vec<S> {
        self.remote.syms.iter().map(|h| h.sym).collect()
    }

    pub fn local_diff(&self) -> Vec<S> {
        self.local.syms.iter().map(|h| h.sym).collect()
    }
}

// ---------- 7.  Tests ------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use rand::{
        SeedableRng,
        rngs::{StdRng, ThreadRng},
    };

    use super::*;

    const TRIALS: usize = 100;

    #[test]
    fn tiny_diff() {
        // This one is not randomized; just run once as before.
        let mut enc = Encoder::<u64>::new();
        for v in 1u64..=10 {
            enc.add_symbol(v);
        }

        let mut dec = Decoder::<u64>::new();
        let bob: [u64; 10] = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        for &b in &bob {
            dec.add_symbol(b);
        }

        loop {
            dec.add_coded_symbol(enc.produce_next());
            dec.try_decode();
            if dec.is_decoded() {
                break;
            }
        }
        assert!(dec.is_decoded(), "decoder failed to converge");

        let mut r = dec.remote_diff();
        r.sort_unstable();
        let mut l = dec.local_diff();
        l.sort_unstable();
        assert_eq!(r, vec![2]);
        assert_eq!(l, vec![11]);
    }

    #[test]
    fn thousand_shared_fifty_delta_each_soak() {
        const COMMON: usize = 1000;
        const DELTA: usize = 5;

        for trial in 0..TRIALS {
            let mut rng = rand::rng();
            let base: Vec<u64> = (0..COMMON).map(|_| rng.random::<u64>()).collect();
            let extra_a: Vec<u64> = (0..DELTA).map(|_| rng.random::<u64>()).collect();
            let extra_b: Vec<u64> = (0..DELTA).map(|_| rng.random::<u64>()).collect();

            let mut enc = Encoder::<u64>::new();
            for &v in base.iter().chain(extra_a.iter()) {
                enc.add_symbol(v)
            }

            let mut dec = Decoder::<u64>::new();
            for &v in base.iter().chain(extra_b.iter()) {
                dec.add_symbol(v)
            }

            loop {
                dec.add_coded_symbol(enc.produce_next());
                dec.try_decode();
                if dec.is_decoded() {
                    // println!("Converged after {} iterations", i + 1);
                    break;
                }
            }

            let mut r = dec.remote_diff();
            r.sort_unstable();
            let mut l = dec.local_diff();
            l.sort_unstable();
            let mut ea = extra_a.clone();
            ea.sort_unstable();
            let mut eb = extra_b.clone();
            eb.sort_unstable();

            if !(dec.is_decoded() && r == ea && l == eb) {
                panic!(
                    "FAILED at trial {}: is_decoded={} remote_diff={:?} local_diff={:?}\nExpected remote={:?} local={:?}",
                    trial,
                    dec.is_decoded(),
                    r,
                    l,
                    ea,
                    eb
                );
            }
        }
    }

    fn gen_unique(
        rng: &mut ThreadRng,
        count: usize,
        forbidden: &mut std::collections::HashSet<u64>,
    ) -> Vec<u64> {
        let mut out = Vec::with_capacity(count);
        while out.len() < count {
            let v = rng.random::<u64>();
            if forbidden.insert(v) {
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn unique_deltas_no_overlap_soak() {
        const COMMON: usize = 1000;
        const DELTA: usize = 5;

        for trial in 0..TRIALS {
            let mut rng = rand::rng();
            let mut taken = std::collections::HashSet::new();

            // common base
            let base: Vec<u64> = gen_unique(&mut rng, COMMON, &mut taken);

            // disjoint deltas
            let extra_a = gen_unique(&mut rng, DELTA, &mut taken);
            let extra_b = gen_unique(&mut rng, DELTA, &mut taken);

            let mut enc = Encoder::<u64>::new();
            for &v in base.iter().chain(extra_a.iter()) {
                enc.add_symbol(v)
            }

            let mut dec = Decoder::<u64>::new();
            for &v in base.iter().chain(extra_b.iter()) {
                dec.add_symbol(v)
            }

            loop {
                dec.add_coded_symbol(enc.produce_next());
                dec.try_decode();
                if dec.is_decoded() {
                    break;
                }
            }
            let mut r = dec.remote_diff();
            r.sort_unstable();
            let mut l = dec.local_diff();
            l.sort_unstable();
            let mut ea = extra_a.clone();
            ea.sort_unstable();
            let mut eb = extra_b.clone();
            eb.sort_unstable();

            if !(dec.is_decoded() && r == ea && l == eb) {
                panic!(
                    "FAILED at trial {}: is_decoded={} remote_diff={:?} local_diff={:?}\nExpected remote={:?} local={:?}",
                    trial,
                    dec.is_decoded(),
                    r,
                    l,
                    ea,
                    eb
                );
            }
        }
    }

    #[test]
    fn overlapping_deltas_cancel_out_soak() {
        const COMMON: usize = 1000;
        const EXCLUSIVE: usize = 4;
        const OVERLAP: usize = 3;

        for trial in 0..TRIALS {
            let mut rng = rand::rng();
            let mut taken = std::collections::HashSet::new();

            let base: Vec<u64> = gen_unique(&mut rng, COMMON, &mut taken);

            // items that appear in both Alice and Bob
            let overlap = gen_unique(&mut rng, OVERLAP, &mut taken);

            // unique to each side
            let extra_a_only = gen_unique(&mut rng, EXCLUSIVE, &mut taken);
            let extra_b_only = gen_unique(&mut rng, EXCLUSIVE, &mut taken);

            let alice = base.iter().chain(overlap.iter()).chain(extra_a_only.iter());
            let bob = base.iter().chain(overlap.iter()).chain(extra_b_only.iter());

            let mut enc = Encoder::<u64>::new();
            for &v in alice {
                enc.add_symbol(v)
            }

            let mut dec = Decoder::<u64>::new();
            for &v in bob {
                dec.add_symbol(v)
            }

            // for _ in 0..((EXCLUSIVE + OVERLAP) * 8) {
            loop {
                dec.add_coded_symbol(enc.produce_next());
                dec.try_decode();
                if dec.is_decoded() {
                    break;
                }
            }
            let mut remote = dec.remote_diff();
            remote.sort_unstable();
            let mut local = dec.local_diff();
            local.sort_unstable();
            let mut a_only = extra_a_only.clone();
            a_only.sort_unstable();
            let mut b_only = extra_b_only.clone();
            b_only.sort_unstable();

            if !(dec.is_decoded() && remote == a_only && local == b_only) {
                panic!(
                    "FAILED at trial {}: is_decoded={} remote_diff={:?} local_diff={:?}\nExpected remote={:?} local={:?}",
                    trial,
                    dec.is_decoded(),
                    remote,
                    local,
                    a_only,
                    b_only
                );
            }
            // sanity: nothing from `overlap` leaks into either diff
            for v in &overlap {
                if remote.contains(v) || local.contains(v) {
                    panic!(
                        "FAILED at trial {}: overlap element {:?} leaked into diffs. Remote: {:?}, Local: {:?}",
                        trial, v, remote, local
                    );
                }
            }
        }
    }
}
