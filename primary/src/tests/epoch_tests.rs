use super::*;
use std::collections::BTreeSet;

#[test]
fn round_robin_segments_cover_each_epoch_exactly_once() {
    for length in [1, 2, 7, 8, 1000] {
        for segments in [1, 3, 4, 10] {
            let mut previous_last = 0;
            for number in 0..3 {
                let epoch = Epoch::new(number, length, segments).unwrap();
                assert_eq!(epoch.first_sn(), previous_last + 1);
                let mut assigned = BTreeSet::new();
                for instance in 0..segments {
                    let segment = epoch.segment(instance).unwrap();
                    assert!(segment.global_sn(0).is_none());
                    assert!(segment.global_sn(segment.slots + 1).is_none());
                    for sequence in 1..=segment.slots {
                        let sn = segment.global_sn(sequence).unwrap();
                        assert!(assigned.insert(sn));
                        assert_eq!((sn - epoch.first_sn()) % segments, instance);
                        let position = Position::for_epoch(number, instance, sequence);
                        assert_eq!(epoch.global_sn(position), Some(sn));
                        assert_eq!(epoch.position(sn), Some(position));
                    }
                }
                assert_eq!(assigned, (epoch.first_sn()..=epoch.last_sn()).collect());
                previous_last = epoch.last_sn();
            }
        }
    }
}

#[test]
fn invalid_layouts_positions_and_overflow_are_rejected() {
    assert!(Epoch::new(0, 0, 4).is_none());
    assert!(Epoch::new(0, 4, 0).is_none());
    assert!(Epoch::new(u64::MAX, 2, 4).is_none());
    assert!(Epoch::new(0, u64::MAX, 4).unwrap().next().is_none());
    let epoch = Epoch::new(2, 7, 4).unwrap();
    assert!(epoch.segment(4).is_none());
    assert!(epoch.position(14).is_none());
    assert!(epoch.position(22).is_none());
    assert!(epoch.global_sn(Position::for_epoch(1, 0, 1)).is_none());
    assert!(epoch.global_sn(Position::for_epoch(2, 3, 2)).is_none());
    assert!(epoch
        .global_sn(Position {
            view: 1,
            ..Position::for_epoch(2, 0, 1)
        })
        .is_none());
}
