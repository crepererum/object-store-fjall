use std::{collections::BTreeSet, fmt::Write, sync::Arc};

use async_trait::async_trait;
use bincode::{Decode, Encode};
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use fjall::{Slice, TransactionalKeyspace, TransactionalPartitionHandle, WriteTransaction};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use get_range::GetRangeExt;
use object_store::{
    Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use uuid::Uuid;

mod get_range;

const STORE: &str = "fjall";
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard()
    .with_little_endian()
    .with_variable_int_encoding()
    .with_no_limit();

struct Handles {
    keyspace: TransactionalKeyspace,
    partition: TransactionalPartitionHandle,
}

impl Handles {
    async fn write_transaction<F>(self: &Arc<Self>, f: F) -> Result<()>
    where
        F: for<'a> Fn(&'a mut WriteTransaction, &'a TransactionalPartitionHandle) -> Result<()>
            + Send
            + 'static,
    {
        let mut f = f;

        loop {
            let this = Arc::clone(self);

            let (done, f_back) = spawn_blocking(move || {
                let Self {
                    keyspace,
                    partition,
                } = this.as_ref();

                let mut tx = keyspace.write_tx().generic_err()?;

                f(&mut tx, partition)?;

                match tx.commit().generic_err()? {
                    Ok(()) => {
                        keyspace
                            .persist(fjall::PersistMode::SyncAll)
                            .generic_err()?;

                        Result::<_, Error>::Ok((true, f))
                    }
                    Err(_) => {
                        // conflict, retry
                        Ok((false, f))
                    }
                }
            })
            .await??;

            f = f_back;

            if done {
                return Ok(());
            }
        }
    }
}

impl std::fmt::Debug for Handles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handles").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct FjallStore {
    handles: Arc<Handles>,
}

impl FjallStore {
    pub async fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<std::path::Path>,
    {
        let config = fjall::Config::new(path);

        let handles = spawn_blocking(move || {
            let keyspace = TransactionalKeyspace::open(config).generic_err()?;
            let partition = keyspace
                .open_partition("object-store", fjall::PartitionCreateOptions::default())
                .generic_err()?;
            Result::<_, Error>::Ok(Handles {
                keyspace,
                partition,
            })
        })
        .await??;

        Ok(Self {
            handles: Arc::new(handles),
        })
    }
}

impl std::fmt::Display for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE}")
    }
}

#[async_trait]
impl ObjectStore for FjallStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutOptions {
            mode,
            // ignore tags
            tags: _,
            attributes,
            // ignore extensions
            extensions: _,
        } = opts;

        // TODO: implement these
        if mode != PutMode::Overwrite {
            return Err(Error::NotImplemented);
        }
        if !attributes.is_empty() {
            return Err(Error::NotImplemented);
        }

        let id = Uuid::new_v4();

        let head_key = head_key(location);
        let head = Head {
            last_modified: Utc::now(),
            size: payload.content_length() as u64,
            id,
        };
        let head_encoded = head.to_slice()?;
        let data_key_prefix = data_key_prefix(head.id);

        self.handles
            .write_transaction(move |tx, partition| {
                let existing = tx
                    .fetch_update(partition, head_key.clone(), |_| {
                        Some(head_encoded.clone().into())
                    })
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, partition, head.id)?;
                }

                for (idx, data) in payload.iter().enumerate() {
                    if data.is_empty() {
                        continue;
                    }

                    let data_key = data_key(data_key_prefix.clone(), idx);
                    tx.insert(partition, data_key, data.clone());
                }

                Ok(())
            })
            .await?;

        let id = id.to_string();

        Ok(PutResult {
            e_tag: Some(id.clone()),
            version: Some(id),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let GetOptions {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            range,
            version,
            head: only_head,
            // ignore extensions
            extensions: _,
        } = options;

        // TODO: implement these
        if if_match.is_some() {
            return Err(Error::NotImplemented);
        }
        if if_none_match.is_some() {
            return Err(Error::NotImplemented);
        }
        if if_modified_since.is_some() {
            return Err(Error::NotImplemented);
        }
        if if_unmodified_since.is_some() {
            return Err(Error::NotImplemented);
        }
        if version.is_some() {
            return Err(Error::NotImplemented);
        }

        let handles = Arc::clone(&self.handles);
        let location = location.clone();
        let head_key = head_key(&location);
        spawn_blocking(move || {
            let Handles {
                keyspace,
                partition,
            } = handles.as_ref();

            let tx = keyspace.read_tx();

            let Some(head) = tx.get(&partition, head_key).generic_err()? else {
                return Err(Error::NotFound {
                    path: location.to_string(),
                    source: "not found".to_owned().into(),
                });
            };
            let head = Head::from_slice(&head)?;

            let range = match range {
                None => 0..head.size,
                Some(range) => range.as_range(head.size).generic_err()?,
            };

            let mut parts = Vec::new();
            if !range.is_empty() && !only_head {
                let mut pos = 0usize;

                let data_key_prefix = data_key_prefix(head.id);
                for kv_res in tx.prefix(&partition, data_key_prefix) {
                    let (_k, v) = kv_res.generic_err()?;

                    let v = Bytes::from(v);
                    let v_sliced = v.slice(
                        (range.start as usize).saturating_sub(pos)
                            ..(range.end as usize).saturating_sub(pos),
                    );
                    if !v_sliced.is_empty() {
                        parts.push(Ok(v_sliced));
                    }
                    pos += v.len();
                }
            }

            Ok(GetResult {
                payload: object_store::GetResultPayload::Stream(
                    futures::stream::iter(parts).boxed(),
                ),
                meta: head.into_meta(location),
                range,
                attributes: Default::default(),
            })
        })
        .await?
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let head_key = head_key(location);

        self.handles
            .write_transaction(move |tx, partition| {
                let existing = tx
                    .fetch_update(partition, head_key.clone(), |_| None)
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, partition, head.id)?;
                }

                Ok(())
            })
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let handles = Arc::clone(&self.handles);
        let head_key_prefix = head_key_prefix(prefix);
        let prefix = prefix.cloned().unwrap_or_default();

        futures::stream::once(async move {
            spawn_blocking(move || {
                let Handles {
                    keyspace,
                    partition,
                } = handles.as_ref();

                let tx = keyspace.read_tx();

                let mut metas = Vec::new();
                for kv_res in tx.prefix(&partition, head_key_prefix) {
                    let (k, v) = kv_res.generic_err()?;
                    let path = path_from_head_key(&k)?;

                    // Don't return for exact prefix match
                    if path
                        .prefix_match(&prefix)
                        .map(|mut x| x.next().is_some())
                        .unwrap_or(false)
                    {
                        metas.push(Ok(Head::from_slice(&v)?.into_meta(path)));
                    }
                }

                Result::<_, Error>::Ok(futures::stream::iter(metas))
            })
            .await?
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let handles = Arc::clone(&self.handles);
        let head_key_prefix = head_key_prefix(prefix);
        let prefix = prefix.cloned().unwrap_or_default();

        spawn_blocking(move || {
            let Handles {
                keyspace,
                partition,
            } = handles.as_ref();

            let tx = keyspace.read_tx();

            let mut common_prefixes = BTreeSet::new();
            let mut objects = vec![];
            for kv_res in tx.prefix(&partition, head_key_prefix) {
                let (k, v) = kv_res.generic_err()?;
                let path = path_from_head_key(&k)?;

                let mut parts = match path.prefix_match(&prefix) {
                    Some(parts) => parts,
                    None => continue,
                };

                // Pop first element
                let common_prefix = match parts.next() {
                    Some(p) => p,
                    // Should only return children of the prefix
                    None => continue,
                };

                if parts.next().is_some() {
                    common_prefixes.insert(prefix.child(common_prefix));
                } else {
                    drop(parts);
                    objects.push(Head::from_slice(&v)?.into_meta(path));
                }
            }

            Ok(ListResult {
                objects,
                common_prefixes: common_prefixes.into_iter().collect(),
            })
        })
        .await?
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let head_key_from = head_key(from);
        let head_key_to = head_key(to);
        let from = from.clone();

        self.handles
            .write_transaction(move |tx, partition| {
                let Some(head_from) = tx.get(partition, head_key_from.clone()).generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };
                let head_from = Head::from_slice(&head_from)?;

                let head_to = Head {
                    id: Uuid::new_v4(),
                    ..head_from
                };
                let head_to_encoded = head_to.to_slice()?;

                let existing = tx
                    .fetch_update(partition, head_key_to.clone(), |_| {
                        Some(head_to_encoded.clone().into())
                    })
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, partition, head.id)?;
                }

                copy_data(tx, partition, head_from.id, head_to.id)?;

                Ok(())
            })
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let head_key_from = head_key(from);
        let head_key_to = head_key(to);
        let from = from.clone();

        self.handles
            .write_transaction(move |tx, partition| {
                let Some(head) = tx
                    .fetch_update(partition, head_key_from.clone(), |_| None)
                    .generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };

                let existing = tx
                    .fetch_update(partition, head_key_to.clone(), |_| Some(head.clone()))
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, partition, head.id)?;
                }

                Ok(())
            })
            .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let head_key_from = head_key(from);
        let head_key_to = head_key(to);
        let from = from.clone();
        let to = to.clone();

        self.handles
            .write_transaction(move |tx, partition| {
                let Some(head_from) = tx.get(partition, head_key_from.clone()).generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };
                let head_from = Head::from_slice(&head_from)?;

                let head_to = Head {
                    id: Uuid::new_v4(),
                    ..head_from
                };
                let head_to_encoded = head_to.to_slice()?;

                let existing = tx
                    .fetch_update(partition, head_key_to.clone(), |v| {
                        Some(v.cloned().unwrap_or_else(|| head_to_encoded.clone()))
                    })
                    .generic_err()?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        path: to.to_string(),
                        source: "already exists".into(),
                    });
                }

                copy_data(tx, partition, head_from.id, head_to.id)?;

                Ok(())
            })
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let head_key_from = head_key(from);
        let head_key_to = head_key(to);
        let from = from.clone();
        let to = to.clone();

        self.handles
            .write_transaction(move |tx, partition| {
                let Some(head) = tx
                    .fetch_update(partition, head_key_from.clone(), |_| None)
                    .generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };

                let existing = tx
                    .fetch_update(partition, head_key_to.clone(), |_| Some(head.clone()))
                    .generic_err()?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        path: to.to_string(),
                        source: "already exists".into(),
                    });
                }

                Ok(())
            })
            .await
    }
}

async fn spawn_blocking<F, R>(f: F) -> Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.generic_err()
}

trait GenericResultExt {
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

fn string_err(s: String) -> Error {
    Error::Generic {
        store: STORE,
        source: s.into(),
    }
}

struct Head {
    last_modified: DateTime<Utc>,
    size: u64,
    id: Uuid,
}

impl Head {
    fn from_slice(s: &[u8]) -> Result<Self> {
        let (this, read) = bincode::decode_from_slice(s, BINCODE_CONFIG).map_err(|e| {
            // why do they not implement std::error::Error?!
            string_err(e.to_string())
        })?;
        if read != s.len() {
            return Err(string_err(format!("trailing bytes: {}", s.len() - read)));
        }
        Ok(this)
    }

    fn to_slice(&self) -> Result<Slice> {
        let data = Bytes::from(bincode::encode_to_vec(self, BINCODE_CONFIG).map_err(|e| {
            // why do they not implement std::error::Error?!
            string_err(e.to_string())
        })?);
        Ok(data.into())
    }

    fn into_meta(self, path: Path) -> ObjectMeta {
        let Head {
            last_modified,
            size,
            id,
        } = self;

        let id = id.to_string();

        ObjectMeta {
            location: path,
            last_modified,
            size,
            e_tag: Some(id.clone()),
            version: Some(id),
        }
    }
}

impl Encode for Head {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> std::result::Result<(), bincode::error::EncodeError> {
        self.last_modified.timestamp().encode(encoder)?;
        self.last_modified
            .timestamp_subsec_nanos()
            .encode(encoder)?;

        self.size.encode(encoder)?;

        self.id.as_u128().encode(encoder)?;

        Ok(())
    }
}

impl<Context> Decode<Context> for Head {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> std::result::Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            last_modified: DateTime::from_timestamp(
                Decode::decode(decoder)?,
                Decode::decode(decoder)?,
            )
            .ok_or(bincode::error::DecodeError::Other("invalid timestamp"))?,
            size: Decode::decode(decoder)?,
            id: Uuid::from_u128(Decode::decode(decoder)?),
        })
    }
}

fn path_from_head_key(key: &[u8]) -> Result<Path> {
    let Some(path) = key.strip_prefix(b"head\0") else {
        return Err(string_err(format!("invalid head key: {key:?}")));
    };
    let path = String::from_utf8(path.to_owned()).generic_err()?;
    let path = Path::parse(path)?;
    Ok(path)
}

/// Scan + delete.
///
/// See <https://github.com/fjall-rs/fjall/issues/33>.
fn clear_data(
    tx: &mut WriteTransaction,
    partition: &TransactionalPartitionHandle,
    id: Uuid,
) -> Result<()> {
    let data_key_prefix = data_key_prefix(id);
    let to_delete = tx
        .prefix(partition, data_key_prefix)
        .map(|res| res.map(|(k, _v)| k).generic_err())
        .collect::<Result<Vec<_>>>()?;

    for k in to_delete {
        tx.remove(partition, k);
    }

    Ok(())
}

fn copy_data(
    tx: &mut WriteTransaction,
    partition: &TransactionalPartitionHandle,
    id_from: Uuid,
    id_to: Uuid,
) -> Result<()> {
    let data_key_prefix_from = data_key_prefix(id_from);
    let data_key_prefix_to = data_key_prefix(id_to);

    let to_copy = tx
        .prefix(&partition, data_key_prefix_from)
        .map(|res| res.map(|(_k, v)| v).generic_err())
        .collect::<Result<Vec<_>>>()?;

    for (idx, data) in to_copy.into_iter().enumerate() {
        let data_key = data_key(data_key_prefix_to.clone(), idx);
        tx.insert(&partition, data_key, data.clone());
    }

    Ok(())
}

fn head_key(location: &Path) -> Slice {
    format!("head\0{location}").into()
}

fn head_key_prefix(prefix: Option<&Path>) -> Slice {
    format!("head\0{}", prefix.map(|p| p.as_ref()).unwrap_or_default()).into()
}

fn data_key_prefix(id: Uuid) -> Slice {
    format!("data\0{id}\0").into()
}

fn data_key(data_key_prefix: Slice, idx: usize) -> Slice {
    let data_key_prefix = Bytes::from(data_key_prefix);
    let mut buf = BytesMut::with_capacity(data_key_prefix.len() + 8);
    buf.put(data_key_prefix);
    buf.write_fmt(format_args!("{idx:08}")).unwrap();
    buf.freeze().into()
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn copy_if_not_exists() {
        let path = tempfile::tempdir().unwrap();
        let storage = FjallStore::open(path.path()).await.unwrap();

        object_store::integration::copy_if_not_exists(&storage).await;
    }

    #[tokio::test]
    async fn get_nonexistent_location() {
        let path = tempfile::tempdir().unwrap();
        let storage = FjallStore::open(path.path()).await.unwrap();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = object_store::integration::get_nonexistent_object(&storage, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, .. } = err {
            assert!(path.ends_with(NON_EXISTENT_NAME), "{}", path);
        } else {
            panic!("unexpected error type: {err:?}");
        }
    }

    #[tokio::test]
    async fn list_uses_directories_correctly() {
        let path = tempfile::tempdir().unwrap();
        let storage = FjallStore::open(path.path()).await.unwrap();

        object_store::integration::list_uses_directories_correctly(&storage).await;
    }

    #[tokio::test]
    async fn list_with_delimiter() {
        let path = tempfile::tempdir().unwrap();
        let storage = FjallStore::open(path.path()).await.unwrap();

        object_store::integration::list_with_delimiter(&storage).await;
    }

    #[tokio::test]
    async fn put_get_delete_list() {
        let path = tempfile::tempdir().unwrap();
        let storage = FjallStore::open(path.path()).await.unwrap();

        object_store::integration::put_get_delete_list(&storage).await;
    }

    #[tokio::test]
    async fn rename_and_copy() {
        let path = tempfile::tempdir().unwrap();
        let storage = FjallStore::open(path.path()).await.unwrap();

        object_store::integration::rename_and_copy(&storage).await;
    }
}
