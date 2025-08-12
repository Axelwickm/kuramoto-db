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
    #[inline]
    pub fn overlaps(&self, other: &RangeCube) -> bool {
        self.intersect(other).is_some()
    }

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

    /// Fast axis-aligned intersection.
    /// Returns `None` if the cubes are disjoint; otherwise returns the tightest
    /// cube that lies inside **both** inputs.
    pub fn intersect(&self, other: &RangeCube) -> Option<RangeCube> {
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

        Some(RangeCube {
            dims: out_dims,
            mins: out_mins,
            maxs: out_maxs,
        })
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

    #[test]
    fn overlaps_and_intersect_basics() {
        let a = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()],
            maxs: smallvec![b"m".to_vec()],
        };
        let b = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"h".to_vec()],
            maxs: smallvec![b"z".to_vec()],
        };
        // overlap
        assert!(a.overlaps(&b));

        let inter = a.intersect(&b).unwrap();
        let expected: SmallVec<[TableHash; 4]> = smallvec![hash(1)];
        assert_eq!(inter.dims, expected);
        assert_eq!(inter.mins[0], b"h".to_vec());
        assert_eq!(inter.maxs[0], b"m".to_vec());
    }

    #[test]
    fn disjoint_on_one_axis() {
        let a = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()],
            maxs: smallvec![b"f".to_vec()],
        };
        let b = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"g".to_vec()],
            maxs: smallvec![b"k".to_vec()],
        };
        assert!(!a.overlaps(&b));
        assert!(a.intersect(&b).is_none());
    }

    #[test]
    fn unconstrained_dim_in_other() {
        // self constrains dim1, other constrains dim2
        let self_cube = RangeCube {
            dims: smallvec![hash(1)],
            mins: smallvec![b"a".to_vec()],
            maxs: smallvec![b"z".to_vec()],
        };
        let other_cube = RangeCube {
            dims: smallvec![hash(2)],
            mins: smallvec![b"c".to_vec()],
            maxs: smallvec![b"d".to_vec()],
        };

        // They overlap because each cube is unbounded on the other's axis.
        assert!(self_cube.overlaps(&other_cube));

        let inter = self_cube.intersect(&other_cube).unwrap();
        // Intersection constrains both axes.
        assert_eq!(inter.dims.len(), 2);
    }

    #[test]
    fn identical_cubes_intersect_to_self() {
        let c = RangeCube {
            dims: smallvec![hash(1), hash(2)],
            mins: smallvec![b"a".to_vec(), b"m".to_vec()],
            maxs: smallvec![b"z".to_vec(), b"z".to_vec()],
        };
        let d = c.clone();
        let inter = c.intersect(&d).unwrap();
        assert_eq!(inter.dims, c.dims);
        assert_eq!(inter.mins, c.mins);
        assert_eq!(inter.maxs, c.maxs);
    }
}
