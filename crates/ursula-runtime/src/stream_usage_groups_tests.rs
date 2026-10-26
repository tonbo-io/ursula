//! The item budget of the per-stream usage export's node-wide read
//! ([`crate::ShardRuntime::stream_usage_groups`]): each group's limit is
//! capped at what the groups before it left, and a group it leaves nothing
//! for is omitted. Hosting and group order are covered end to end.

use std::num::NonZeroUsize;

use crate::runtime::group_read_limit;

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("a positive limit")
}

#[test]
fn a_group_reads_its_limit_capped_at_what_the_answer_has_left() {
    // The first group gets its whole limit when the budget holds it.
    assert_eq!(group_read_limit(limit(10), 20, 0), Some(limit(10)));
    assert_eq!(group_read_limit(limit(10), 10, 0), Some(limit(10)));
    // A later group gets what the groups before it left.
    assert_eq!(group_read_limit(limit(10), 20, 13), Some(limit(7)));
    assert_eq!(group_read_limit(limit(10), 5, 4), Some(limit(1)));
    // Nothing left: the group is omitted.
    assert_eq!(group_read_limit(limit(10), 4, 4), None);
    assert_eq!(group_read_limit(limit(10), 4, 9), None);
}
