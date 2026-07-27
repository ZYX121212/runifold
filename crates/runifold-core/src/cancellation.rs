use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use event_listener::Event;

/// A hierarchical cancellation token.
///
/// Cancelling a parent is visible to all descendants. Cancelling a child does
/// not affect its parent or siblings.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

#[derive(Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    event: Event,
    children: Mutex<Vec<Weak<CancellationState>>>,
}

impl CancellationToken {
    /// Creates an uncancelled root token.
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                event: Event::new(),
                children: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Creates a child token linked to this token.
    #[must_use]
    pub fn child_token(&self) -> Self {
        let mut children = self
            .state
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let child = Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(self.is_cancelled()),
                event: Event::new(),
                children: Mutex::new(Vec::new()),
            }),
        };
        if !child.is_cancelled() {
            children.push(Arc::downgrade(&child.state));
        }
        child
    }

    /// Cancels this token and, transitively, its descendants.
    pub fn cancel(&self) {
        cancel_state(&self.state);
    }

    /// Returns whether this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Waits until this token or one of its ancestors is cancelled.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let listener = self.state.event.listen();
            if self.is_cancelled() {
                return;
            }
            listener.await;
        }
    }
}

fn cancel_state(state: &Arc<CancellationState>) {
    if state.cancelled.swap(true, Ordering::AcqRel) {
        return;
    }
    state.event.notify(usize::MAX);

    let children = state
        .children
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter_map(Weak::upgrade)
        .collect::<Vec<_>>();
    for child in children {
        cancel_state(&child);
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::CancellationToken;

    #[test]
    fn parent_cancellation_reaches_descendants() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let grandchild = child.child_token();

        parent.cancel();

        assert!(parent.is_cancelled());
        assert!(child.is_cancelled());
        assert!(grandchild.is_cancelled());
    }

    #[test]
    fn child_cancellation_is_isolated() {
        let parent = CancellationToken::new();
        let first_child = parent.child_token();
        let second_child = parent.child_token();

        first_child.cancel();

        assert!(first_child.is_cancelled());
        assert!(!parent.is_cancelled());
        assert!(!second_child.is_cancelled());
    }

    #[test]
    fn asynchronous_waiters_are_woken_by_ancestor_cancellation() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let waiter = child.clone();
        let canceller = std::thread::spawn(move || parent.cancel());

        futures_executor::block_on(waiter.cancelled());
        canceller.join().unwrap();

        assert!(child.is_cancelled());
    }

    #[test]
    fn children_created_after_cancellation_start_cancelled() {
        let parent = CancellationToken::new();
        parent.cancel();

        let child = parent.child_token();

        assert!(child.is_cancelled());
        futures_executor::block_on(child.cancelled());
    }
}
