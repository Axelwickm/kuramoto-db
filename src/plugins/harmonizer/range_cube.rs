use std::cmp::Ordering;

use bincode::{Decode, Encode};
use smallvec::SmallVec;

use crate::tables::TableHash;

/// Axis-aligned cube over ≤4 dimensions (hash-ids)
#[derive(Clone, Debug, Eq, PartialEq, Encode, Decode)]
pub struct RangeCube {
    // private: enforce construction via `new`
    dims: SmallVec<[TableHash; 4]>, // which index tables participate
    mins: SmallVec<[Vec<u8>; 4]>,   // same order as dims
    maxs: SmallVec<[Vec<u8>; 4]>,   // same order
}

impl RangeCube {
    /// Read-only accessors
    pub fn dims(&self) -> &[TableHash] { &self.dims }
    pub fn mins(&self) -> &[Vec<u8>] { &self.mins }
    pub fn maxs(&self) -> &[Vec<u8>] { &self.maxs }
    pub fn len(&self) -> usize { self.dims.len() }
    pub fn set_min(&mut self, idx: usize, v: Vec<u8>) { self.mins[idx] = v }
    pub fn set_max(&mut self, idx: usize, v: Vec<u8>) { self.maxs[idx] = v }

    /// Construct a well-formed cube: lengths must match and dims are sorted ascending by hash.
    /// `mins` and `maxs` are reordered to match the sorted dims.
    pub fn new(
        dims: SmallVec<[TableHash; 4]>,
        mins: SmallVec<[Vec<u8>; 4]>,
        maxs: SmallVec<[Vec<u8>; 4]>,
    ) -> Result<Self, &'static str> {
        if dims.len() != mins.len() || dims.len() != maxs.len() {
            return Err("dims/mins/maxs length mismatch");
        }
        // Zip and sort by dim hash, then unzip back to SmallVecs.
        let mut tuples: Vec<(TableHash, Vec<u8>, Vec<u8>)> =
            dims.into_iter().zip(mins).zip(maxs).map(|((d, mi), ma)| (d, mi, ma)).collect();
        tuples.sort_by_key(|t| t.0.hash);

        let mut out_dims = SmallVec::<[TableHash; 4]>::with_capacity(tuples.len());
        let mut out_mins = SmallVec::<[Vec<u8>; 4]>::with_capacity(tuples.len());
        let mut out_maxs = SmallVec::<[Vec<u8>; 4]>::with_capacity(tuples.len());
        for (d, mi, ma) in tuples {
            out_dims.push(d);
            out_mins.push(mi);
            out_maxs.push(ma);
        }
        Ok(Self { dims: out_dims, mins: out_mins, maxs: out_maxs })
    }

    #[inline]
    fn debug_assert_invariants(&self) {
        debug_assert!(self.dims.len() == self.mins.len() && self.dims.len() == self.maxs.len());
        debug_assert!(self
            .dims
            .windows(2)
            .all(|w| w[0].hash <= w[1].hash), "RangeCube.dims must be sorted by hash");
    }

    #[inline]
    pub fn overlaps(&self, other: &RangeCube) -> bool {
        self.debug_assert_invariants();
        other.debug_assert_invariants();
        self.intersect(other).is_some()
    }

    /// Assumes `dims` are sorted ascending (enforced on constructor)
    pub fn contains(&self, other: &RangeCube) -> bool {
        self.debug_assert_invariants();
        other.debug_assert_invariants();
        // walk two sorted lists in O(N+M)
        let mut i = 0;
        let mut j = 0;
        while i < self.dims.len() && j < other.dims.len() {
            match self.dims[i].hash.cmp(&other.dims[j].hash) {
                Ordering::Less => {
                    i += 1;
                } // self dim not in other → unbounded
                Ordering::Greater => {
                    return false;
                } // other constrained on a dim we don’t have
                Ordering::Equal => {
                    if other.mins[j] < self.mins[i] || other.maxs[j] > self.maxs[i] {
                        return false;
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
        j == other.dims.len() // no missing dims in `other`
    }

    /// Fast axis-aligned intersection.
    /// Returns `None` if the cubes are disjoint; otherwise returns the tightest
    /// cube that lies inside **both** inputs.
    pub fn intersect(&self, other: &RangeCube) -> Option<RangeCube> {
        self.debug_assert_invariants();
        other.debug_assert_invariants();
        // Early-reject if either dims list is empty and the other has a
        // disjoint bound on at least one axis.  We’ll detect that while
        // scanning, so no extra work here.

        // Pre-allocate only if we know there *is* an overlap.
        // We accumulate mins/maxs lazily in two small stacks.
        let mut out_dims = SmallVec::<[TableHash; 4]>::new();
        let mut out_mins = SmallVec::<[Vec<u8>; 4]>::new();
        let mut out_maxs = SmallVec::<[Vec<u8>; 4]>::new();

        let mut i = 0;
        let mut j = 0;

        // Merge-walk the two sorted dim lists
        while i < self.dims.len() || j < other.dims.len() {
            match (self.dims.get(i), other.dims.get(j)) {
                (Some(da), Some(db)) => match da.hash.cmp(&db.hash) {
                    Ordering::Less => {
                        // Only `self` constrains this axis.
                        out_dims.push(*da);
                        out_mins.push(self.mins[i].clone());
                        out_maxs.push(self.maxs[i].clone());
                        i += 1;
                    }
                    Ordering::Greater => {
                        // Only `other` constrains this axis.
                        out_dims.push(*db);
                        out_mins.push(other.mins[j].clone());
                        out_maxs.push(other.maxs[j].clone());
                        j += 1;
                    }
                    Ordering::Equal => {
                        // Both constrain → tighten to overlap.
                        let lo = std::cmp::max(&self.mins[i], &other.mins[j]);
                        let hi = std::cmp::min(&self.maxs[i], &other.maxs[j]);
                        if lo > hi {
                            // Disjoint on this axis → whole cubes are disjoint
                            return None;
                        }
                        out_dims.push(*da);
                        out_mins.push(lo.clone());
                        out_maxs.push(hi.clone());
                        i += 1;
                        j += 1;
                    }
                },
                (Some(_), None) => {
                    // Remaining dims only in `self`
                    out_dims.push(self.dims[i]);
                    out_mins.push(self.mins[i].clone());
                    out_maxs.push(self.maxs[i].clone());
                    i += 1;
                }
                (None, Some(_)) => {
                    // Remaining dims only in `other`
                    out_dims.push(other.dims[j]);
                    out_mins.push(other.mins[j].clone());
                    out_maxs.push(other.maxs[j].clone());
                    j += 1;
                }
                (None, None) => unreachable!(),
            }
        }

        Some(RangeCube { dims: out_dims, mins: out_mins, maxs: out_maxs })
    }

    /// TODO: test
    /// Very rough "volume" proxy used only for tie-breaking.
    /// Uses sum of per-dim (len(max) - len(min)) in lexicographic byte space.
    pub fn approx_volume(&self) -> u128 {
        self.debug_assert_invariants();
        let n = self.mins.len().min(self.maxs.len());
        let mut acc: i128 = 0;
        for i in 0..n {
            acc += (self.maxs[i].len() as i128) - (self.mins[i].len() as i128);
        }
        if acc < 0 { 0 } else { acc as u128 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    // Convenient helper for brevity
    fn hash(id: u64) -> TableHash {
        TableHash { hash: id }
    }

    #[test]
    fn identical_cubes_contain_each_other() {
        let c1 = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![b"a".to_vec(), b"m".to_vec()],
            smallvec![b"z".to_vec(), b"z".to_vec()],
        )
        .unwrap();
        let c2 = c1.clone();

        assert!(c1.contains(&c2));
        assert!(c2.contains(&c1));
    }

    #[test]
    fn strict_subset_is_contained() {
        // self has wider bounds than other on both dims
        let wide = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![b"a".to_vec(), b"m".to_vec()],
            smallvec![b"z".to_vec(), b"z".to_vec()],
        )
        .unwrap();
        let narrow = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![b"c".to_vec(), b"o".to_vec()],
            smallvec![b"x".to_vec(), b"y".to_vec()],
        )
        .unwrap();

        assert!(wide.contains(&narrow));
        assert!(!narrow.contains(&wide));
    }

    #[test]
    fn extra_dim_in_other_fails() {
        // `other` constrains dim 3 that `self` doesn’t track
        let self_cube = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![b"a".to_vec(), b"a".to_vec()],
            smallvec![b"z".to_vec(), b"z".to_vec()],
        )
        .unwrap();
        let other_cube = RangeCube::new(
            smallvec![hash(1), hash(2), hash(3)],
            smallvec![b"b".to_vec(), b"b".to_vec(), b"b".to_vec()],
            smallvec![b"y".to_vec(), b"y".to_vec(), b"y".to_vec()],
        )
        .unwrap();

        assert!(!self_cube.contains(&other_cube));
    }

    #[test]
    fn self_unbounded_extra_dim_is_ok() {
        // `self` has extra dim, `other` ignores it → still contained
        let bigger = RangeCube::new(
            smallvec![hash(1), hash(2), hash(3)],
            smallvec![b"a".to_vec(), b"a".to_vec(), b"a".to_vec()],
            smallvec![b"z".to_vec(), b"z".to_vec(), b"z".to_vec()],
        )
        .unwrap();
        let smaller = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![b"b".to_vec(), b"b".to_vec()],
            smallvec![b"y".to_vec(), b"y".to_vec()],
        )
        .unwrap();

        assert!(bigger.contains(&smaller));
    }

    #[test]
    fn touching_bounds_are_inclusive() {
        // other.min == self.min and other.max == self.max should succeed
        let outer = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"z".to_vec()],
        )
        .unwrap();
        let exact_edges = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"z".to_vec()],
        )
        .unwrap();

        assert!(outer.contains(&exact_edges));
    }

    #[test]
    fn out_of_bounds_fails() {
        let outer = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"c".to_vec()],
            smallvec![b"x".to_vec()],
        )
        .unwrap();
        let overreach = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()], // too small
            smallvec![b"x".to_vec()],
        )
        .unwrap();
        let underreach = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"c".to_vec()],
            smallvec![b"z".to_vec()], // too large
        )
        .unwrap();

        assert!(!outer.contains(&overreach));
        assert!(!outer.contains(&underreach));
    }

    #[test]
    fn overlaps_and_intersect_basics() {
        let a = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"m".to_vec()],
        )
        .unwrap();
        let b = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"h".to_vec()],
            smallvec![b"z".to_vec()],
        )
        .unwrap();
        // overlap
        assert!(a.overlaps(&b));

        let inter = a.intersect(&b).unwrap();
        let expected: SmallVec<[TableHash; 4]> = smallvec![hash(1)];
        assert_eq!(inter.dims(), &expected[..]);
        assert_eq!(inter.mins()[0], b"h".to_vec());
        assert_eq!(inter.maxs()[0], b"m".to_vec());
    }

    #[test]
    fn disjoint_on_one_axis() {
        let a = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"f".to_vec()],
        )
        .unwrap();
        let b = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"g".to_vec()],
            smallvec![b"k".to_vec()],
        )
        .unwrap();
        assert!(!a.overlaps(&b));
        assert!(a.intersect(&b).is_none());
    }

    #[test]
    fn unconstrained_dim_in_other() {
        // self constrains dim1, other constrains dim2
        let self_cube = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"z".to_vec()],
        )
        .unwrap();
        let other_cube = RangeCube::new(
            smallvec![hash(2)],
            smallvec![b"c".to_vec()],
            smallvec![b"d".to_vec()],
        )
        .unwrap();

        // They overlap because each cube is unbounded on the other's axis.
        assert!(self_cube.overlaps(&other_cube));

        let inter = self_cube.intersect(&other_cube).unwrap();
        // Intersection constrains both axes.
        assert_eq!(inter.dims().len(), 2);
    }

    #[test]
    fn identical_cubes_intersect_to_self() {
        let c = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![b"a".to_vec(), b"m".to_vec()],
            smallvec![b"z".to_vec(), b"z".to_vec()],
        )
        .unwrap();
        let d = c.clone();
        let inter = c.intersect(&d).unwrap();
        assert_eq!(inter.dims(), c.dims());
        assert_eq!(inter.mins(), c.mins());
        assert_eq!(inter.maxs(), c.maxs());
    }

    #[test]
    fn approx_volume_adds_across_dims_and_clamps_negative() {
        use smallvec::smallvec;

        // 1) Single-dimension: span = len(max) - len(min) = 3 - 1 = 2
        let one = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"a\x02\x00".to_vec()],
        )
        .unwrap();
        assert_eq!(one.approx_volume(), 2);

        // 2) Two dims: (1 - 0) + (2 - 1) = 2 total
        let two = RangeCube::new(
            smallvec![hash(1), hash(2)],
            smallvec![vec![], b"a".to_vec()],
            smallvec![vec![0xFF], b"a\x01".to_vec()],
        )
        .unwrap();
        assert_eq!(two.approx_volume(), 2);

        // 3) Negative span should clamp to 0
        let neg = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"abc".to_vec()], // len = 3
            smallvec![b"a".to_vec()],   // len = 1 → 1 - 3 = -2 → clamp to 0
        )
        .unwrap();
        assert_eq!(neg.approx_volume(), 0);

        // 4) Equal lengths → 0
        let eq = RangeCube::new(
            smallvec![hash(1)],
            smallvec![b"a".to_vec()],
            smallvec![b"a".to_vec()],
        )
        .unwrap();
        assert_eq!(eq.approx_volume(), 0);
    }
}
