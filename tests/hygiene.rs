use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{DynObjectStore, ObjectStore, PutPayload, path::Path};
use object_store_fjall::FjallStore;

#[tokio::test]
async fn copy_without_override() {
    assert_hygiene(async |storage, payload| {
        let path_a = Path::parse("a").unwrap();
        let path_b = Path::parse("b").unwrap();

        storage.put(&path_a, payload).await.unwrap();
        storage.copy(&path_a, &path_b).await.unwrap();
        storage.delete(&path_a).await.unwrap();
        storage.delete(&path_b).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn copy_with_override() {
    assert_hygiene(async |storage, payload| {
        let path_a = Path::parse("a").unwrap();
        let path_b = Path::parse("b").unwrap();

        storage.put(&path_a, payload.clone()).await.unwrap();
        storage.put(&path_b, payload.clone()).await.unwrap();
        storage.copy(&path_a, &path_b).await.unwrap();
        storage.delete(&path_a).await.unwrap();
        storage.delete(&path_b).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn put_once() {
    assert_hygiene(async |storage, payload| {
        let path = Path::parse("a").unwrap();

        storage.put(&path, payload).await.unwrap();
        storage.delete(&path).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn put_twice() {
    assert_hygiene(async |storage, payload| {
        let path = Path::parse("a").unwrap();

        storage.put(&path, payload.clone()).await.unwrap();
        storage.put(&path, payload.clone()).await.unwrap();
        storage.delete(&path).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn rename_without_override() {
    assert_hygiene(async |storage, payload| {
        let path_a = Path::parse("a").unwrap();
        let path_b = Path::parse("b").unwrap();

        storage.put(&path_a, payload).await.unwrap();
        storage.rename(&path_a, &path_b).await.unwrap();
        storage.delete(&path_b).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn rename_with_override() {
    assert_hygiene(async |storage, payload| {
        let path_a = Path::parse("a").unwrap();
        let path_b = Path::parse("b").unwrap();

        storage.put(&path_a, payload.clone()).await.unwrap();
        storage.put(&path_b, payload.clone()).await.unwrap();
        storage.rename(&path_a, &path_b).await.unwrap();
        storage.delete(&path_b).await.unwrap();
    })
    .await;
}

async fn assert_hygiene<F>(f: F)
where
    F: for<'a> AsyncFnOnce(&'a DynObjectStore, PutPayload),
{
    let path = tempfile::tempdir().unwrap();
    let storage = FjallStore::open(path.path()).await.unwrap();

    let payload = [Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        .into_iter()
        .collect::<PutPayload>();

    f(&storage, payload).await;

    let stats = storage.stats().await.unwrap();
    assert_eq!(stats.head_len, 0);
    assert_eq!(stats.data_len, 0);

    let paths = storage
        .list(None)
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(paths, Vec::new());
}
