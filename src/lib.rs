use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;
use constants::STORE;
use error::{GenericResultExt, string_err};
use fjall::{Slice, TransactionalKeyspace, TransactionalPartitionHandle, WriteTransaction};
use futures::{Stream, StreamExt, stream::BoxStream};
use object_store::{
    Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use serialization::{Head, WrappedAttributes};
use tokio::{sync::mpsc::Receiver, task::JoinSet};
use uuid::Uuid;

mod constants;
mod error;
mod serialization;

struct Partitions {
    head: TransactionalPartitionHandle,
    data: TransactionalPartitionHandle,
}

struct Handles {
    keyspace: TransactionalKeyspace,
    partitions: Partitions,
}

impl Handles {
    async fn write_transaction<F>(self: &Arc<Self>, f: F) -> Result<()>
    where
        F: for<'a> Fn(&'a mut WriteTransaction, &'a Partitions) -> Result<()> + Send + 'static,
    {
        let mut f = f;

        loop {
            let this = Arc::clone(self);

            let (done, f_back) = spawn_blocking(move || {
                let Self {
                    keyspace,
                    partitions,
                } = this.as_ref();

                let mut tx = keyspace.write_tx().generic_err()?;

                f(&mut tx, partitions)?;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FjallStatistics {
    /// Number of head entries.
    pub head_len: usize,

    /// Disk space used by the "head" partition.
    pub head_disk_space: u64,

    /// Number of data entries.
    pub data_len: usize,

    /// Disk space used by the "data" partition.
    pub data_disk_space: u64,
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
        let config = fjall::Config::new(path)
            .manual_journal_persist(true)
            .max_write_buffer_size(128 * 1024 * 1024);

        let handles = spawn_blocking(move || {
            let keyspace = TransactionalKeyspace::open(config).generic_err()?;

            let head = keyspace
                .open_partition(
                    "head",
                    fjall::PartitionCreateOptions::default()
                        .compression(fjall::CompressionType::None)
                        .manual_journal_persist(true),
                )
                .generic_err()?;

            let data = keyspace
                .open_partition(
                    "data",
                    fjall::PartitionCreateOptions::default()
                        .bloom_filter_bits(None)
                        .compression(fjall::CompressionType::None)
                        .manual_journal_persist(true)
                        .max_memtable_size(64 * 1024 * 1024)
                        .with_kv_separation(fjall::KvSeparationOptions::default()),
                )
                .generic_err()?;

            Result::<_, Error>::Ok(Handles {
                keyspace,
                partitions: Partitions { head, data },
            })
        })
        .await??;

        Ok(Self {
            handles: Arc::new(handles),
        })
    }

    pub async fn stats(&self) -> Result<FjallStatistics> {
        let handles = Arc::clone(&self.handles);
        spawn_blocking(move || {
            let Handles {
                keyspace: _,
                partitions,
            } = handles.as_ref();
            let Partitions { head, data } = partitions;

            let head = head.inner();
            let head_len = head.len().generic_err()?;
            let head_disk_space = head.disk_space();

            let data = data.inner();
            let data_len = data.len().generic_err()?;
            let data_disk_space = data.disk_space();

            Ok(FjallStatistics {
                head_len,
                head_disk_space,
                data_len,
                data_disk_space,
            })
        })
        .await?
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

        let id = Uuid::now_v7();

        let head_key = head_key(location);
        let head = Head {
            last_modified: Utc::now(),
            size: payload.content_length() as u64,
            id,
        };
        let head_encoded = head.to_slice()?;
        let data_base = data_base(head.id);
        let data_key_prefix = data_key_prefix(data_base.clone());

        self.handles
            .write_transaction(move |tx, partitions| {
                let existing = tx
                    .fetch_update(&partitions.head, head_key.clone(), |_| {
                        Some(head_encoded.clone())
                    })
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, &partitions.data, head.id)?;
                }

                tx.insert(
                    &partitions.data,
                    data_base.clone(),
                    WrappedAttributes::from(attributes.clone())
                        .to_slice()
                        .generic_err()?,
                );

                for (idx, data) in payload.iter().enumerate() {
                    if data.is_empty() {
                        continue;
                    }

                    let data_key = data_key(data_key_prefix.clone(), idx as u64);
                    tx.insert(&partitions.data, data_key, data.clone());
                }

                Ok(())
            })
            .await?;

        Ok(PutResult {
            e_tag: Some(id.to_string()),
            version: None,
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
        let handles = Arc::clone(&self.handles);
        let location = location.clone();
        let head_key = head_key(&location);
        spawn_blocking(move || {
            let Handles {
                keyspace,
                partitions,
            } = handles.as_ref();

            let tx = keyspace.read_tx();

            let Some(head) = tx.get(&partitions.head, head_key).generic_err()? else {
                return Err(Error::NotFound {
                    path: location.to_string(),
                    source: "not found".to_owned().into(),
                });
            };
            let head = Head::from_slice(&head)?;
            let meta = head.object_meta(location.clone());
            options.check_preconditions(&meta)?;

            let range = match options.range {
                None => 0..head.size,
                Some(range) => range.as_range(head.size).generic_err()?,
            };

            let data_base = data_base(head.id);
            let attributes = WrappedAttributes::from_slice(
                tx.get(&partitions.data, data_base.clone())
                    .generic_err()?
                    .ok_or_else(|| string_err("attributes missing".to_owned()))?
                    .as_ref(),
            )?
            .into();

            let stream = if !range.is_empty() && !options.head {
                // Producer and consumer now run in two different threads and context switching is expensive, so we
                // want to avoid that. Hence we give the buffer some head room.
                let (sender, rx) = tokio::sync::mpsc::channel(5);

                let mut task = JoinSet::new();
                task.spawn_blocking(move || {
                    let Handles {
                        keyspace: _,
                        partitions,
                    } = handles.as_ref();

                    let mut pos = 0usize;
                    let data_key_prefix = data_key_prefix(data_base);
                    for kv_res in tx.prefix(&partitions.data, data_key_prefix) {
                        let v = match kv_res {
                            Ok((_k, v)) => v,
                            Err(e) => {
                                sender.blocking_send(Err(e).generic_err()).ok();
                                return;
                            }
                        };

                        let v = Bytes::from(v);
                        let v_range = (range.start as usize).saturating_sub(pos)
                            ..(range.end as usize).saturating_sub(pos).min(v.len());
                        if !v_range.is_empty()
                            && sender.blocking_send(Ok(v.slice(v_range))).is_err()
                        {
                            return;
                        }
                        pos += v.len();
                    }
                });

                GetStream { _task: task, rx }.boxed()
            } else {
                futures::stream::empty().boxed()
            };

            Ok(GetResult {
                payload: object_store::GetResultPayload::Stream(stream),
                meta,
                range,
                attributes,
            })
        })
        .await?
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let head_key = head_key(location);

        self.handles
            .write_transaction(move |tx, partitions| {
                let existing = tx
                    .fetch_update(&partitions.head, head_key.clone(), |_| None)
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, &partitions.data, head.id)?;
                }

                Ok(())
            })
            .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let handles = Arc::clone(&self.handles);
        let head_key_prefix = head_key_prefix(prefix);
        let prefix = prefix.cloned().unwrap_or_default();
        let mut task = JoinSet::new();

        // Producer and consumer now run in two different threads and context switching is expensive, so we
        // want to avoid that. Hence we give the buffer some head room.
        let (sender, rx) = tokio::sync::mpsc::channel(100);

        task.spawn_blocking(move || {
            let Handles {
                keyspace,
                partitions,
            } = handles.as_ref();

            let tx = keyspace.read_tx();

            for kv_res in tx.prefix(&partitions.head, head_key_prefix) {
                let (k, v) = match kv_res {
                    Ok((k, v)) => (k, v),
                    Err(e) => {
                        sender.blocking_send(Err(e).generic_err()).ok();
                        return;
                    }
                };
                let path = match path_from_head_key(&k) {
                    Ok(path) => path,
                    Err(e) => {
                        sender.blocking_send(Err(e).generic_err()).ok();
                        return;
                    }
                };

                // Don't return for exact prefix match
                if path
                    .prefix_match(&prefix)
                    .map(|mut x| x.next().is_some())
                    .unwrap_or(false)
                {
                    let head = match Head::from_slice(&v) {
                        Ok(head) => head,
                        Err(e) => {
                            sender.blocking_send(Err(e).generic_err()).ok();
                            return;
                        }
                    };
                    if sender.blocking_send(Ok(head.object_meta(path))).is_err() {
                        return;
                    }
                }
            }
        });

        ListStream { _task: task, rx }.boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let handles = Arc::clone(&self.handles);
        let head_key_prefix = head_key_prefix(prefix);
        let prefix = prefix.cloned().unwrap_or_default();

        spawn_blocking(move || {
            let Handles {
                keyspace,
                partitions,
            } = handles.as_ref();

            let tx = keyspace.read_tx();

            let mut common_prefixes = BTreeSet::new();
            let mut objects = vec![];
            for kv_res in tx.prefix(&partitions.head, head_key_prefix) {
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
                    objects.push(Head::from_slice(&v)?.object_meta(path));
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
            .write_transaction(move |tx, partitions| {
                let Some(head_from) = tx
                    .get(&partitions.head, head_key_from.clone())
                    .generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };
                let head_from = Head::from_slice(&head_from)?;

                let head_to = Head {
                    id: Uuid::now_v7(),
                    ..head_from
                };
                let head_to_encoded = head_to.to_slice()?;

                let existing = tx
                    .fetch_update(&partitions.head, head_key_to.clone(), |_| {
                        Some(head_to_encoded.clone())
                    })
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, &partitions.data, head.id)?;
                }

                copy_data(tx, &partitions.data, head_from.id, head_to.id)?;

                Ok(())
            })
            .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let head_key_from = head_key(from);
        let head_key_to = head_key(to);
        let from = from.clone();

        self.handles
            .write_transaction(move |tx, partitions| {
                let Some(head) = tx
                    .fetch_update(&partitions.head, head_key_from.clone(), |_| None)
                    .generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };

                let existing = tx
                    .fetch_update(&partitions.head, head_key_to.clone(), |_| {
                        Some(head.clone())
                    })
                    .generic_err()?;

                if let Some(head) = existing {
                    let head = Head::from_slice(&head)?;
                    clear_data(tx, &partitions.data, head.id)?;
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
            .write_transaction(move |tx, partitions| {
                let Some(head_from) = tx
                    .get(&partitions.head, head_key_from.clone())
                    .generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };
                let head_from = Head::from_slice(&head_from)?;

                let head_to = Head {
                    id: Uuid::now_v7(),
                    ..head_from
                };
                let head_to_encoded = head_to.to_slice()?;

                let existing = tx
                    .fetch_update(&partitions.head, head_key_to.clone(), |v| {
                        Some(v.cloned().unwrap_or_else(|| head_to_encoded.clone()))
                    })
                    .generic_err()?;

                if existing.is_some() {
                    return Err(Error::AlreadyExists {
                        path: to.to_string(),
                        source: "already exists".into(),
                    });
                }

                copy_data(tx, &partitions.data, head_from.id, head_to.id)?;

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
            .write_transaction(move |tx, partitions| {
                let Some(head) = tx
                    .fetch_update(&partitions.head, head_key_from.clone(), |_| None)
                    .generic_err()?
                else {
                    return Err(Error::NotFound {
                        path: from.to_string(),
                        source: "not found".to_owned().into(),
                    });
                };

                let existing = tx
                    .fetch_update(&partitions.head, head_key_to.clone(), |_| {
                        Some(head.clone())
                    })
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

fn path_from_head_key(key: &[u8]) -> Result<Path> {
    let path = String::from_utf8(key.to_owned()).generic_err()?;
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
    let data_base = data_base(id);
    tx.remove(partition, data_base.clone());

    let to_delete = tx
        .prefix(partition, data_key_prefix(data_base))
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
    let data_base_from = data_base(id_from);
    let data_base_to = data_base(id_to);

    // attributes
    let attrs = tx
        .get(partition, data_base_from.clone())
        .generic_err()?
        .ok_or_else(|| string_err("attributes missing".to_owned()))?;
    tx.insert(partition, data_base_to.clone(), attrs);

    let data_key_prefix_from = data_key_prefix(data_base_from);
    let data_key_prefix_to = data_key_prefix(data_base_to);

    let to_copy = tx
        .prefix(partition, data_key_prefix_from)
        .map(|res| res.map(|(_k, v)| v).generic_err())
        .collect::<Result<Vec<_>>>()?;

    for (idx, data) in to_copy.into_iter().enumerate() {
        let data_key = data_key(data_key_prefix_to.clone(), idx as u64);
        tx.insert(partition, data_key, data.clone());
    }

    Ok(())
}

fn head_key(location: &Path) -> Slice {
    head_key_prefix(Some(location))
}

fn head_key_prefix(prefix: Option<&Path>) -> Slice {
    match prefix {
        Some(p) => Bytes::copy_from_slice(p.as_ref().as_bytes()).into(),
        None => Bytes::new().into(),
    }
}

fn data_base(id: Uuid) -> Slice {
    Bytes::copy_from_slice(&id.as_u128().to_le_bytes()).into()
}

fn data_key_prefix(data_base: Slice) -> Slice {
    let data_base = Bytes::from(data_base);
    let mut buf = BytesMut::with_capacity(data_base.len() + 1);
    buf.put(data_base);
    buf.put_u8(0);
    buf.freeze().into()
}

fn data_key(data_key_prefix: Slice, idx: u64) -> Slice {
    let data_key_prefix = Bytes::from(data_key_prefix);
    let mut buf = BytesMut::with_capacity(data_key_prefix.len() + 64 / 8);
    buf.put(data_key_prefix);
    buf.put_slice(&idx.to_be_bytes());
    buf.freeze().into()
}

struct GetStream {
    _task: JoinSet<()>,
    rx: Receiver<Result<Bytes>>,
}

impl Stream for GetStream {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct ListStream {
    _task: JoinSet<()>,
    rx: Receiver<Result<ObjectMeta>>,
}

impl Stream for ListStream {
    type Item = Result<ObjectMeta>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_key_sorting() {
        let prefix = data_key_prefix(data_base(Uuid::from_u128(1337)));

        let key_0 = data_key(prefix.clone(), 0);
        let key_1 = data_key(prefix.clone(), 1);
        let key_u8_max = data_key(prefix.clone(), u8::MAX as u64);
        let key_u8_max_plus_1 = data_key(prefix.clone(), u8::MAX as u64 + 1);
        let key_u64_max_minus_1 = data_key(prefix.clone(), u64::MAX - 1);
        let key_u64_max = data_key(prefix.clone(), u64::MAX);

        println!("              key_0: {key_0:?}");
        println!("              key_1: {key_1:?}");
        println!("         key_u8_max: {key_u8_max:?}");
        println!("  key_u8_max_plus_1: {key_u8_max_plus_1:?}");
        println!("key_u64_max_minus_1: {key_u64_max_minus_1:?}");
        println!("        key_u64_max: {key_u64_max:?}");

        assert!(key_0 < key_1);
        assert!(key_1 < key_u8_max);
        assert!(key_u8_max < key_u8_max_plus_1);
        assert!(key_u8_max_plus_1 < key_u64_max_minus_1);
        assert!(key_u64_max_minus_1 < key_u64_max);
    }
}
