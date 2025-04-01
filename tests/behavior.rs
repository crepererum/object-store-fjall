use std::ops::Range;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{GetOptions, ObjectStore, path::Path};
use object_store_fjall::FjallStore;

#[tokio::test]
async fn test_multiple_blocks() {
    let path = tempfile::tempdir().unwrap();
    let storage = FjallStore::open(path.path()).await.unwrap();

    let path = Path::parse("x").unwrap();
    let payload = [
        Bytes::from_static(b"foo"),
        Bytes::from_static(b"bar"),
        Bytes::from_static(b"x"),
    ]
    .into_iter()
    .collect();
    storage.put(&path, payload).await.unwrap();

    let get_range = async |range: Option<Range<u64>>| {
        storage
            .get_opts(
                &path,
                GetOptions {
                    range: range.map(|r| r.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    };

    assert_eq!(
        get_range(None).await,
        [
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"x")
        ]
    );
    assert_eq!(
        get_range(Some(0..7)).await,
        [
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"x")
        ]
    );
    assert_eq!(
        // too long
        get_range(Some(0..8)).await,
        [
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"x")
        ]
    );
    assert_eq!(
        get_range(Some(1..7)).await,
        [
            Bytes::from_static(b"oo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"x")
        ]
    );
    assert_eq!(
        get_range(Some(3..7)).await,
        [Bytes::from_static(b"bar"), Bytes::from_static(b"x")]
    );
    assert_eq!(
        get_range(Some(1..6)).await,
        [Bytes::from_static(b"oo"), Bytes::from_static(b"bar"),]
    );
    assert_eq!(
        get_range(Some(1..5)).await,
        [Bytes::from_static(b"oo"), Bytes::from_static(b"ba"),]
    );
}

#[tokio::test]
async fn test_empty_blocks() {
    let path = tempfile::tempdir().unwrap();
    let storage = FjallStore::open(path.path()).await.unwrap();

    let path = Path::parse("x").unwrap();
    let payload = [
        Bytes::from_static(b""),
        Bytes::from_static(b"foo"),
        Bytes::from_static(b""),
        Bytes::from_static(b"bar"),
        Bytes::from_static(b""),
    ]
    .into_iter()
    .collect();
    storage.put(&path, payload).await.unwrap();

    let actual = storage
        .get(&path)
        .await
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let expected = [Bytes::from_static(b"foo"), Bytes::from_static(b"bar")];
    assert_eq!(actual, expected);
}
