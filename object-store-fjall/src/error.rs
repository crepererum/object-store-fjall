//! Error handling.

use object_store::{Error, Result};

use crate::STORE;

pub(crate) trait GenericResultExt {
    type T;

    fn generic_err(self) -> Result<Self::T>;
}

impl<T, E> GenericResultExt for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type T = T;

    fn generic_err(self) -> Result<Self::T> {
        self.map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })
    }
}

pub(crate) fn string_err(s: String) -> Error {
    Error::Generic {
        store: STORE,
        source: s.into(),
    }
}
