use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use bincode::{Decode, Encode};

// Feature flag for debug printing
#[cfg(feature = "riblt_debug")]
macro_rules! debug_print {
    ($($arg:tt)*) => {
        println!($($arg)*);
    };
}

#[cfg(not(feature = "riblt_debug"))]
macro_rules! debug_print {
    ($($arg:tt)*) => {};
}

#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

#[inline]
fn hash_bytes<const BYTES: usize>(bytes: &[u8; BYTES]) -> u64 {
    let mut x = 0u64;
    for (i, b) in bytes.iter().enumerate().take(8) {
        x |= (*b as u64) << (8 * i);
    }
    splitmix64(x)
}

#[inline]
fn xor_bytes_inplace<const N: usize>(a: &mut [u8; N], b: &[u8; N]) {
    for i in 0..N {
        a[i] ^= b[i];
    }
}

/// User data item must expose stable key bytes.
pub trait Symbol<const BYTES: usize>: Copy + Eq + Hash + Debug {
    fn encode(&self) -> [u8; BYTES];
    fn decode(_: [u8; BYTES]) -> Self;
}

/// Per-symbol state: holds one 64-bit seed *and* the running index.
#[derive(Clone, Copy, Debug)]
pub struct SymbolMappingGenerator<const BYTES: usize, S: Symbol<BYTES>> {
    seed: u64,
    next_idx: usize,
    hash: u64,
    bytes: [u8; BYTES],
    _ty: std::marker::PhantomData<S>,
}

impl<const BYTES: usize, S: Symbol<BYTES>> SymbolMappingGenerator<BYTES, S> {
    pub fn new(sym: &S) -> Self {
        let encoded = sym.encode();
        let bytes_hash = hash_bytes(&encoded);
        let mut h = std::collections::hash_map::DefaultHasher::new();
        h.write_u64(bytes_hash);
        debug_print!("Creating SMG, bytes {:?}, hash {}", encoded, bytes_hash);
        let seed = h.finish();
        Self {
            seed,
            next_idx: 0,
            hash: bytes_hash,
            bytes: encoded,
            _ty: std::marker::PhantomData,
        }
    }

    /// returns (xor, hash) **and advances** to the next hit
    pub fn pop(&mut self) -> ([u8; BYTES], u64, usize) {
        let idx = self.next_idx;
        let bytes = self.bytes;
        let hash = self.hash;
        self.seed = splitmix64(self.seed);
        let tp32 = 1u64 << 32;
        let gap = ((idx as f64) + 1.5) * ((tp32 as f64) / ((self.seed as f64) + 1.0).sqrt() - 1.0);
        self.next_idx += gap.ceil() as usize;
        (bytes, hash, idx)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CodedSymbol<const BYTES: usize, S: Symbol<BYTES>> {
    pub count: i64,
    pub xor: [u8; BYTES],
    pub hash: u64,
    pub _ty: std::marker::PhantomData<S>,
}

impl<const BYTES: usize, S: Symbol<BYTES>> CodedSymbol<BYTES, S> {
    fn diff(&mut self, bytes: &[u8; BYTES], hash: u64, count: i64) {
        self.count += count;
        xor_bytes_inplace(&mut self.xor, bytes);
        self.hash ^= hash;
    }
}

pub struct Encoder<const BYTES: usize, S: Symbol<BYTES>> {
    maps: Vec<SymbolMappingGenerator<BYTES, S>>,
    heap: BinaryHeap<Reverse<(usize, usize)>>, // heap: (cursor,  symbol map idx)
    cursor: usize,
}

impl<const BYTES: usize, S: Symbol<BYTES>> Encoder<BYTES, S> {
    pub fn new(symbols: &[S]) -> Self {
        let mut maps: Vec<SymbolMappingGenerator<BYTES, S>> = Vec::with_capacity(symbols.len());
        let mut heap = BinaryHeap::with_capacity(symbols.len());
        for (i, sym) in symbols.iter().enumerate() {
            let mut smg = SymbolMappingGenerator::new(sym);
            let (_xor, _hash, idx) = smg.pop();
            assert_eq!(
                idx, 0,
                "First mapped index for every symbol must be 0, got {}",
                idx
            );
            maps.push(smg);
            heap.push(Reverse((idx, i)));
        }
        Self {
            maps,
            heap,
            cursor: 0,
        }
    }

    pub fn next_coded(&mut self) -> CodedSymbol<BYTES, S> {
        let mut coded_symbol = CodedSymbol::<BYTES, S> {
            count: 0,
            xor: [0u8; BYTES],
            hash: 0,
            _ty: std::marker::PhantomData,
        };
        debug_print!("Heap {}", self.heap.len());
        while let Some(&Reverse((idx, map_id))) = self.heap.peek() {
            debug_print!("Heap idx {}, map_id {}", idx, map_id);
            if idx != self.cursor {
                break;
            }
            self.heap.pop();
            let (xor, h, new_idx) = self.maps[map_id].pop();
            debug_print!("xor in {:?}, {}, {}", xor, h, new_idx);
            coded_symbol.count += 1;
            xor_bytes_inplace(&mut coded_symbol.xor, &xor);
            coded_symbol.hash ^= h;
            debug_print!("coded_symbol xor {:?}", coded_symbol.xor);
            self.heap.push(Reverse((new_idx, map_id)));
        }
        debug_print!("");
        self.cursor += 1;
        coded_symbol
    }

    pub fn seek(&mut self, target: usize) {
        while self.cursor < target {
            self.next_coded(); // generate & throw away
        }
    }
}

pub struct Decoder<const BYTES: usize, S: Symbol<BYTES>> {
    local_encoder: Encoder<BYTES, S>,
    remote_coded_symbols: Vec<CodedSymbol<BYTES, S>>,
    peeled: Vec<(SymbolMappingGenerator<BYTES, S>, i64)>,
    seen: HashSet<S>,
    local_only: HashSet<S>,
    remote_only: HashSet<S>,
    is_decoded: bool,
}

impl<const BYTES: usize, S: Symbol<BYTES>> Decoder<BYTES, S> {
    pub fn new(local: &[S]) -> Self {
        Self {
            local_encoder: Encoder::new(local),
            remote_coded_symbols: vec![],
            peeled: vec![],
            seen: HashSet::new(),
            local_only: HashSet::new(),
            remote_only: HashSet::new(),
            is_decoded: false,
        }
    }

    pub fn finish(self) -> (HashSet<S>, HashSet<S>) {
        (self.remote_only, self.local_only)
    }

    fn ingest(&mut self, remote: CodedSymbol<BYTES, S>) {
        debug_print!("Ingest Cursor {}", self.local_encoder.cursor);
        debug_print!(
            "Ingested remote: count {}, xor {:?}, hash {:?}",
            remote.count,
            remote.xor,
            remote.hash
        );
        let local = self.local_encoder.next_coded();
        let mut diff = remote.clone();
        diff.diff(&local.xor, local.hash, -local.count);

        // replay every symbol we have already peeled *once* on this index
        for p in &mut self.peeled {
            if p.0.next_idx == self.local_encoder.cursor {
                let (bytes, _h, _idx) = p.0.pop();
                diff.diff(&bytes, p.0.hash, -p.1);
            }
        }

        let mut peel_q: VecDeque<usize> = VecDeque::new();
        let chash = diff.hash == hash_bytes::<BYTES>(&diff.xor);
        if chash && matches!(diff.count, -1 | 1) {
            debug_print!(
                "Newly ingested symbol is peelable at {}",
                self.local_encoder.cursor - 1
            );
            peel_q.push_back(self.local_encoder.cursor - 1);
        }

        self.remote_coded_symbols.push(diff);

        while let Some(peel_idx) = peel_q.pop_front() {
            debug_print!("New peel at {}", peel_idx);
            let (mut smg, count) = {
                let count = self.remote_coded_symbols[peel_idx].count;
                let xor = self.remote_coded_symbols[peel_idx].xor;
                let hash = self.remote_coded_symbols[peel_idx].hash;
                let rs = &mut self.remote_coded_symbols[peel_idx];
                debug_print!("Peel at xor {:?}", xor);

                if !matches!(count, -1 | 0 | 1) {
                    continue;
                }
                if count == 0 {
                    continue;
                }

                let peel_sym = S::decode(xor);
                if !self.seen.insert(peel_sym) {
                    continue;
                }

                let smg = SymbolMappingGenerator::<BYTES, S>::new(&peel_sym);
                self.peeled.push((smg, count));
                if count == 1 {
                    debug_print!("add to remote only, idx {} {:?}", peel_idx, peel_sym);
                    self.remote_only.insert(peel_sym);
                } else if count == -1 {
                    debug_print!("add to local only, idx {} {:?}", peel_idx, peel_sym);
                    rs.diff(&xor, hash, -count);
                    self.local_only.insert(peel_sym);
                }
                (smg, count)
            };

            loop {
                let (bytes, _h, idx) = smg.pop();
                let idx = idx as usize;
                debug_print!("Peel {} subpeels at {}", peel_idx, idx);
                if idx >= self.remote_coded_symbols.len() {
                    debug_print!("Passed materialised, breaking");
                    break;
                }
                if self.remote_coded_symbols[idx].hash == 0 {
                    debug_print!("Hash at {} is zero, so already processed", idx);
                    continue;
                }
                self.remote_coded_symbols[idx].diff(&bytes, smg.hash, -count);
                debug_print!(
                    "subpeeled new count: {}, hash {}, xor {:?}",
                    self.remote_coded_symbols[idx].count,
                    hash_bytes::<BYTES>(&self.remote_coded_symbols[idx].xor),
                    &self.remote_coded_symbols[idx].xor
                );
                let chash = self.remote_coded_symbols[idx].hash
                    == hash_bytes::<BYTES>(&self.remote_coded_symbols[idx].xor);
                if chash && matches!(self.remote_coded_symbols[idx].count, -1 | 1) {
                    debug_print!(
                        "Sub-peel reveal idx {}, count {} , hash {}, xor hash {}, xor {:?}",
                        idx,
                        self.remote_coded_symbols[idx].count,
                        smg.hash,
                        hash_bytes::<BYTES>(&self.remote_coded_symbols[idx].xor),
                        &self.remote_coded_symbols[idx].xor
                    );
                    peel_q.push_back(idx);
                }
                if idx == self.local_encoder.cursor {
                    break;
                }
            }
        }

        if self.remote_coded_symbols[0].count == 0
            && self.remote_coded_symbols[0].hash == 0
            && self.remote_coded_symbols[0].xor.iter().all(|&b| b == 0)
        {
            self.is_decoded = true;
        }
    }
}

impl Symbol<8> for u64 {
    fn encode(&self) -> [u8; 8] {
        self.to_le_bytes()
    }
    fn decode(bytes: [u8; 8]) -> Self {
        u64::from_le_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use rand::rngs::ThreadRng;

    type U64Enc = Encoder<8, u64>;
    type U64Dec = Decoder<8, u64>;
    type Cell = CodedSymbol<8, u64>;

    const TRIALS: usize = 1;

    #[test]
    fn tiny_diff() {
        let alice: Vec<u64> = (1..=10).collect(); // local encoder
        let bob: Vec<u64> = [3, 4, 5, 7, 8, 9, 10, 11, 12, 15, 16].into();

        let mut enc = U64Enc::new(&alice);
        let mut dec = U64Dec::new(&bob);

        while !dec.is_decoded {
            let c: Cell = enc.next_coded();
            dec.ingest(c);
        }
        assert!(dec.is_decoded);

        let (remote, local) = dec.finish();

        let expected_remote: HashSet<_> = [2u64, 6, 1].into_iter().collect();
        assert_eq!(remote, expected_remote);

        let expected_local: HashSet<_> = [11u64, 12, 15, 16].into_iter().collect();
        assert_eq!(local, expected_local);
    }

    #[test]
    fn thousand_shared_hundred_each() {
        const COMMON: usize = 100000;
        const DELTA: usize = 1000;

        for _ in 0..TRIALS {
            let mut rng = rand::rng();

            let base: Vec<u64> = (0..COMMON).map(|_| rng.random()).collect();
            let extra_a: Vec<u64> = (0..DELTA).map(|_| rng.random()).collect();
            let extra_b: Vec<u64> = (0..DELTA).map(|_| rng.random()).collect();

            let mut enc = U64Enc::new(
                &base
                    .iter()
                    .chain(extra_a.iter())
                    .copied()
                    .collect::<Vec<_>>(),
            );
            let mut dec = U64Dec::new(
                &base
                    .iter()
                    .chain(extra_b.iter())
                    .copied()
                    .collect::<Vec<_>>(),
            );

            while !dec.is_decoded {
                let c: Cell = enc.next_coded();
                dec.ingest(c);
            }

            assert!(dec.is_decoded);

            let (remote, local) = dec.finish();

            let ea: HashSet<_> = extra_a.into_iter().collect();
            let eb: HashSet<_> = extra_b.into_iter().collect();

            assert_eq!(remote, ea);
            assert_eq!(local, eb);
        }
    }

    fn gen_unique(
        rng: &mut ThreadRng,
        count: usize,
        taken: &mut std::collections::HashSet<u64>,
    ) -> Vec<u64> {
        let mut out = Vec::with_capacity(count);
        while out.len() < count {
            let v = rng.random::<u64>();
            if taken.insert(v) {
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn unique_deltas_no_overlap() {
        const COMMON: usize = 1000;
        const DELTA: usize = 5;

        for _ in 0..TRIALS {
            let mut rng = rand::rng();
            let mut taken = std::collections::HashSet::new();

            let base = gen_unique(&mut rng, COMMON, &mut taken);
            let extra_a = gen_unique(&mut rng, DELTA, &mut taken);
            let extra_b = gen_unique(&mut rng, DELTA, &mut taken);

            let mut enc = U64Enc::new(
                &base
                    .iter()
                    .chain(extra_a.iter())
                    .copied()
                    .collect::<Vec<_>>(),
            );
            let mut dec = U64Dec::new(
                &base
                    .iter()
                    .chain(extra_b.iter())
                    .copied()
                    .collect::<Vec<_>>(),
            );

            while !dec.is_decoded {
                let c: Cell = enc.next_coded();
                dec.ingest(c);
            }

            assert!(dec.is_decoded);

            let (remote, local) = dec.finish();

            let ea: HashSet<_> = extra_a.into_iter().collect();
            let eb: HashSet<_> = extra_b.into_iter().collect();

            assert_eq!(remote, ea);
            assert_eq!(local, eb);
        }
    }

    #[test]
    fn overlapping_deltas_cancel_out() {
        const COMMON: usize = 200;
        const EXCLUSIVE: usize = 100;

        for _ in 0..TRIALS {
            let mut rng = rand::rng();
            let mut taken = std::collections::HashSet::new();

            let base = gen_unique(&mut rng, COMMON, &mut taken);
            let extra_a = gen_unique(&mut rng, EXCLUSIVE, &mut taken);
            let extra_b = gen_unique(&mut rng, EXCLUSIVE, &mut taken);

            let alice_vec: Vec<u64> = base.iter().chain(extra_a.iter()).copied().collect();
            let bob_vec: Vec<u64> = base.iter().chain(extra_b.iter()).copied().collect();

            let mut enc = U64Enc::new(&alice_vec);
            let mut dec = U64Dec::new(&bob_vec);

            while !dec.is_decoded {
                let c: Cell = enc.next_coded();
                dec.ingest(c);
            }

            assert!(dec.is_decoded);

            let (remote, local) = dec.finish();

            let ea: HashSet<_> = extra_a.into_iter().collect();
            let eb: HashSet<_> = extra_b.into_iter().collect();

            assert_eq!(remote, ea);
            assert_eq!(local, eb);

            for v in &base {
                assert!(!remote.contains(v) && !local.contains(v));
            }
        }
    }
}
