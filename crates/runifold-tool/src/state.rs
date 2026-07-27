use std::{ops::Deref, sync::Arc};

/// Shared application state injected into a typed Tool handler.
///
/// State is host-only. It is not represented in the Tool's input schema,
/// model arguments, transcript, or write-ahead Effect input.
pub struct State<T: ?Sized>(Arc<T>);

impl<T> State<T> {
    /// Wraps owned application state for shared injection.
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T: ?Sized> State<T> {
    /// Wraps existing shared application state.
    pub const fn from_shared(value: Arc<T>) -> Self {
        Self(value)
    }

    /// Returns the shared state allocation.
    pub fn shared(&self) -> &Arc<T> {
        &self.0
    }

    /// Consumes the wrapper and returns the shared state allocation.
    pub fn into_shared(self) -> Arc<T> {
        self.0
    }
}

impl<T: ?Sized> Clone for State<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: ?Sized> Deref for State<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> std::fmt::Debug for State<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("State")
            .field(&std::any::type_name::<T>())
            .finish()
    }
}
