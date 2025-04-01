use object_store::{Error, GetOptions, ObjectMeta, Result};

pub(crate) trait GetOptionsExt {
    /// Returns an error if the modification conditions on this request are not satisfied
    ///
    /// <https://datatracker.ietf.org/doc/html/rfc7232#section-6>
    fn check_preconditions(&self, meta: &ObjectMeta) -> Result<()>;
}

impl GetOptionsExt for GetOptions {
    fn check_preconditions(&self, meta: &ObjectMeta) -> Result<()> {
        // The use of the invalid etag "*" means no ETag is equivalent to never matching
        let etag = meta.e_tag.as_deref().unwrap_or("*");
        let last_modified = meta.last_modified;

        if let Some(m) = &self.if_match {
            if m != "*" && m.split(',').map(str::trim).all(|x| x != etag) {
                return Err(Error::Precondition {
                    path: meta.location.to_string(),
                    source: format!("{etag} does not match {m}").into(),
                });
            }
        } else if let Some(date) = self.if_unmodified_since {
            if last_modified > date {
                return Err(Error::Precondition {
                    path: meta.location.to_string(),
                    source: format!("{date} < {last_modified}").into(),
                });
            }
        }

        if let Some(m) = &self.if_none_match {
            if m == "*" || m.split(',').map(str::trim).any(|x| x == etag) {
                return Err(Error::NotModified {
                    path: meta.location.to_string(),
                    source: format!("{etag} matches {m}").into(),
                });
            }
        } else if let Some(date) = self.if_modified_since {
            if last_modified <= date {
                return Err(Error::NotModified {
                    path: meta.location.to_string(),
                    source: format!("{date} >= {last_modified}").into(),
                });
            }
        }
        Ok(())
    }
}
