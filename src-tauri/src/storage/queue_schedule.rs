//! Deciding what order queue items run in, and which may run together
//! (backlog phase 2.2).
//!
//! Draining a queue strictly in row order is safe but slow. Draining it
//! concurrently is fast but wrong, because some operations depend on others:
//!
//! - A directory placeholder must exist **before** the files inside it, so
//!   creates run parents-first.
//! - A directory can only be removed once it is empty, so deletes run
//!   children-first — the exact reverse.
//! - A rename must land before anything writes to the name it frees, and after
//!   anything still reading the name it takes.
//!
//! Content transfers, by contrast, touch one path each and are independent.
//! That is the whole opportunity: the transfers are the slow part *and* the
//! parallelizable part.
//!
//! So the queue is cut into ordered **stages**. Stages run strictly one after
//! another; within a stage, items are independent by construction and may run
//! concurrently up to the global transfer budget. Anything this module cannot
//! prove independent goes in a sequential stage — being slow is recoverable,
//! being wrong is not.
//!
//! Tauri-free and pure so the ordering rules can be tested directly, which is
//! where the bugs would otherwise hide.

/// How a stage's items may be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageMode {
    /// Items touch independent paths; run them concurrently.
    Concurrent,
    /// Items have ordering dependencies; run them one at a time, in order.
    Sequential,
}

/// One ordered group of queue items.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueueStage<T> {
    pub mode: StageMode,
    pub items: Vec<T>,
}

/// What an item needs from the scheduler's point of view.
pub(crate) trait Schedulable {
    fn operation(&self) -> &str;
    fn path(&self) -> &str;
    /// Bytes to move, if this is a transfer. Used only for ordering.
    fn transfer_size(&self) -> Option<u64>;
}

/// Split `items` into stages that are safe to run in the order returned.
///
/// The stage order is: create directories (parents first) → content transfers
/// (concurrent) → structural operations (sequential) → deletes (children
/// first).
///
/// Deletes run last so that a plan which both removes an old path and writes a
/// new one never destroys data it has not finished copying. Structural
/// operations (renames, conflict duplication, anchor reconciliation) run
/// sequentially between them because their dependencies are not expressible
/// from the path alone.
pub(crate) fn plan_stages<T: Schedulable>(items: Vec<T>) -> Vec<QueueStage<T>> {
    let mut creates = Vec::new();
    let mut transfers = Vec::new();
    let mut structural = Vec::new();
    let mut deletes = Vec::new();

    for item in items {
        match item.operation() {
            "create_directory" => creates.push(item),
            "upload" | "download" => transfers.push(item),
            "delete_remote" | "delete_local" => deletes.push(item),
            // Renames, conflict duplication, anchor reconciliation, and any
            // operation added later that this module has not been taught
            // about. Unknown means sequential: a new operation must not
            // silently inherit permission to run concurrently.
            _ => structural.push(item),
        }
    }

    // Parents before children: "a/" must exist before "a/b/".
    creates.sort_by(|left, right| depth_then_path(left).cmp(&depth_then_path(right)));

    // Smallest first. Total time is unchanged, but the queue visibly drains
    // from the start instead of stalling behind one large file — and a failure
    // that stops the run has cost less work.
    transfers.sort_by(|left, right| {
        left.transfer_size()
            .unwrap_or(0)
            .cmp(&right.transfer_size().unwrap_or(0))
            .then_with(|| left.path().cmp(right.path()))
    });

    // Children before parents, so a directory is empty by the time it is
    // removed. Exactly the reverse of the create ordering.
    deletes.sort_by(|left, right| depth_then_path(right).cmp(&depth_then_path(left)));

    [
        QueueStage {
            mode: StageMode::Sequential,
            items: creates,
        },
        QueueStage {
            mode: StageMode::Concurrent,
            items: transfers,
        },
        QueueStage {
            mode: StageMode::Sequential,
            items: structural,
        },
        QueueStage {
            mode: StageMode::Sequential,
            items: deletes,
        },
    ]
    .into_iter()
    .filter(|stage| !stage.items.is_empty())
    .collect()
}

/// Sort key: depth first, then path, so ordering is total and deterministic.
///
/// Depth alone would leave siblings in arbitrary order, which makes failures
/// irreproducible; the path tiebreak costs nothing and makes runs repeatable.
fn depth_then_path<T: Schedulable>(item: &T) -> (usize, &str) {
    let path = item.path();
    (path.matches('/').count(), path)
}

#[cfg(test)]
mod tests {
    use super::{plan_stages, QueueStage, Schedulable, StageMode};

    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        operation: &'static str,
        path: &'static str,
        size: Option<u64>,
    }

    impl Schedulable for TestItem {
        fn operation(&self) -> &str {
            self.operation
        }
        fn path(&self) -> &str {
            self.path
        }
        fn transfer_size(&self) -> Option<u64> {
            self.size
        }
    }

    fn item(operation: &'static str, path: &'static str) -> TestItem {
        TestItem {
            operation,
            path,
            size: None,
        }
    }

    fn sized(operation: &'static str, path: &'static str, size: u64) -> TestItem {
        TestItem {
            operation,
            path,
            size: Some(size),
        }
    }

    fn paths<T: Schedulable>(stage: &QueueStage<T>) -> Vec<&str> {
        stage.items.iter().map(|item| item.path()).collect()
    }

    #[test]
    fn directories_are_created_parents_first() {
        let stages = plan_stages(vec![
            item("create_directory", "a/b/c"),
            item("create_directory", "a"),
            item("create_directory", "a/b"),
        ]);

        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].mode, StageMode::Sequential);
        // Creating "a/b/c" before "a" would fail or strand the placeholder.
        assert_eq!(paths(&stages[0]), vec!["a", "a/b", "a/b/c"]);
    }

    #[test]
    fn deletes_run_children_first() {
        let stages = plan_stages(vec![
            item("delete_remote", "a"),
            item("delete_remote", "a/b/c"),
            item("delete_remote", "a/b"),
        ]);

        assert_eq!(stages.len(), 1);
        // Removing "a" first would leave its children orphaned or fail.
        assert_eq!(paths(&stages[0]), vec!["a/b/c", "a/b", "a"]);
    }

    #[test]
    fn transfers_are_the_only_concurrent_stage() {
        let stages = plan_stages(vec![
            item("create_directory", "a"),
            item("upload", "a/f.txt"),
            item("move_remote", "old.txt"),
            item("delete_remote", "gone.txt"),
        ]);

        let modes: Vec<StageMode> = stages.iter().map(|stage| stage.mode).collect();
        assert_eq!(
            modes,
            vec![
                StageMode::Sequential, // creates
                StageMode::Concurrent, // transfers
                StageMode::Sequential, // structural
                StageMode::Sequential, // deletes
            ]
        );
    }

    #[test]
    fn creates_precede_transfers_and_deletes_come_last() {
        let stages = plan_stages(vec![
            item("delete_local", "old/f.txt"),
            item("download", "new/f.txt"),
            item("create_directory", "new"),
        ]);

        let order: Vec<&str> = stages
            .iter()
            .flat_map(|stage| paths(stage))
            .collect::<Vec<_>>();
        // The directory must exist before the file lands in it, and nothing is
        // deleted until the replacement content has been written.
        assert_eq!(order, vec!["new", "new/f.txt", "old/f.txt"]);
    }

    #[test]
    fn an_unknown_operation_is_treated_as_sequential_rather_than_concurrent() {
        // A new operation added later must not silently inherit permission to
        // run concurrently just because nobody updated this file.
        let stages = plan_stages(vec![item("teleport_sideways", "x")]);

        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].mode, StageMode::Sequential);
        assert_eq!(paths(&stages[0]), vec!["x"]);
    }

    #[test]
    fn small_transfers_go_first_so_the_queue_visibly_drains() {
        let stages = plan_stages(vec![
            sized("upload", "huge.iso", 8_000_000_000),
            sized("upload", "note.txt", 12),
            sized("download", "photo.jpg", 4_000_000),
        ]);

        assert_eq!(paths(&stages[0]), vec!["note.txt", "photo.jpg", "huge.iso"]);
    }

    #[test]
    fn ordering_is_deterministic_for_equal_keys() {
        // Irreproducible ordering makes a failure impossible to investigate.
        let build = || {
            vec![
                sized("upload", "b.txt", 10),
                sized("upload", "a.txt", 10),
                sized("upload", "c.txt", 10),
            ]
        };
        assert_eq!(
            paths(&plan_stages(build())[0]),
            paths(&plan_stages(build())[0])
        );
        assert_eq!(
            paths(&plan_stages(build())[0]),
            vec!["a.txt", "b.txt", "c.txt"]
        );
    }

    #[test]
    fn empty_stages_are_omitted_entirely() {
        let stages = plan_stages(vec![item("upload", "only.txt")]);
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].mode, StageMode::Concurrent);

        assert!(plan_stages(Vec::<TestItem>::new()).is_empty());
    }

    #[test]
    fn no_item_is_ever_dropped_or_duplicated() {
        let items = vec![
            item("create_directory", "d"),
            item("upload", "d/a"),
            item("download", "d/b"),
            item("move_remote", "d/c"),
            item("delete_remote", "d/e"),
            item("delete_local", "d/f"),
            item("something_new", "d/g"),
        ];
        let expected = items.len();

        let scheduled: usize = plan_stages(items).iter().map(|s| s.items.len()).sum();

        // Losing a queue item means an operation silently never happens.
        assert_eq!(scheduled, expected);
    }
}
