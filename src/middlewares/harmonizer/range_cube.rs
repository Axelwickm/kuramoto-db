use std::cmp::Ordering;

use bincode::{Decode, Encode};
use smallvec::SmallVec;

use crate::tables::TableHash;

/// Axis-aligned cube over ≤4 dimensions (hash-ids)
#[derive(Clone, Debug, Encode, Decode)]
pub struct RangeCube {
    pub dims: SmallVec<[TableHash; 4]>, // which index tables participate
    pub mins: SmallVec<[Vec<u8>; 4]>,   // same order as dims
    pub maxs: SmallVec<[Vec<u8>; 4]>,   // same order
}

impl RangeCube {
    /// Assumes `dims` are sorted ascending (enforced on constructor)
    pub fn contains(&self, other: &RangeCube) -> bool {
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
        let c1 = RangeCube {
            dims: smallvec![hash(1), hash(2)],
            mins: smallvec![b"a".to_vec(), b"m".to_vec()],
            maxs: smallvec![b"z".to_vec(), b"z".to_vec()],
        };
        let c2 = c1.clone();

        assert!(c1.contains(&c2));
        assert!(c2.contains(&c1));
    }

    #[test]
    fn strict_subset_is_contained() {
        // self has wider bounds than other on both dims
        let wide = RangeCube {
            dims: smallvec![hash(1), hash(2)],
            mins: smallvec![b"a".to_vec(), b"m".to_vec()],
            maxs: smallvec![b"z".to_vec(), b"z".to_vec()],
        };
        let narrow = RangeCube {
            dims: smallvec![hash(1), hash(2)],
            mins: smallvec![b"c".to_vec(), b"o".to_vec()],
            maxs: smallvec![b"x".to_vec(), b"y".to_vec()],
        };

        assert!(wide.contains(&narrow));
        assert!(!narrow.contains(&wide));
    }

    #[test]
    fn extra_dim_in_other_fails() {
        // `other` constrains dim 3 that `self` doesn’t track
        let self_cube = RangeCube {
            dims: smallvec![hash(1), hash(2)],
            mins: smallvec![b"a".to_vec(), b"a".to_vec()],
            maxs: smallvec![b"z".to_vec(), b"z".to_vec()],
        };
        let other_cube = RangeCube {
            dims: smallvec![hash(1), hash(2), hash(3)],
            mins: smallvec![b"b".to_vec(), b"b".to_vec(), b"b".to_vec()],
            maxs: smallvec![b"y".to_vec(), b"y".to_vec(), b"y".to_vec()],
        };

        assert!(!self_cube.contains(&other_cube));
    }

    #[test]
    fn self_unbounded_extra_dim_is_ok() {
        // `self` has extra dim, `other` ignores it → still contained
        let bigger = RangeCube {
            dims: smallvec![hash(1), hash(2), hash(3)],
            mins: smallvec![b"a".to_vec(), b"a".to_vec(), b"a".to_vec()],
            maxs: smallvec![b"z".to_vec(), b"z".to_vec(), b"z".to_vec()],
        };
        let smaller = RangeCube {
            dims: smallvec![hash(1), hash(2)],
            mins: smallvec![b"b".to_vec(), b"b".to_vec()],
            maxs: smallvec![b"y".to_vec(), b"y".to_vec()],
        };

        assert!(bigger.contains(&smaller));
    }

    #[test]
    fn touching_bounds_are_inclusive() {
        // other.min == self.min and other.max == self.max should succeed
        let outer = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()],
            maxs: smallvec![b"z".to_vec()],
        };
        let exact_edges = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()],
            maxs: smallvec![b"z".to_vec()],
        };

        assert!(outer.contains(&exact_edges));
    }

    #[test]
    fn out_of_bounds_fails() {
        let outer = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"c".to_vec()],
            maxs: smallvec![b"x".to_vec()],
        };
        let overreach = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()], // too small
            maxs: smallvec![b"x".to_vec()],
        };
        let underreach = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"c".to_vec()],
            maxs: smallvec![b"z".to_vec()], // too large
        };

        assert!(!outer.contains(&overreach));
        assert!(!outer.contains(&underreach));
    }

    #[test]
    fn empty_other_is_always_contained() {
        // An empty `dims` set means “no constraints” → always inside.
        let big = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()],
            maxs: smallvec![b"z".to_vec()],
        };
        let unconstrained = RangeCube {
            dims: smallvec![],
            mins: smallvec![],
            maxs: smallvec![],
        };

        assert!(big.contains(&unconstrained));
    }
}
