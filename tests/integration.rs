//! Upstream integration tests.

use object_store::path::Path;
use object_store_fjall::FjallStore;

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
    if let object_store::Error::NotFound { path, .. } = err {
        assert!(path.ends_with(NON_EXISTENT_NAME), "{}", path);
    } else {
        panic!("unexpected error type: {err:?}");
    }
}

#[tokio::test]
async fn get_opts() {
    let path = tempfile::tempdir().unwrap();
    let storage = FjallStore::open(path.path()).await.unwrap();

    object_store::integration::get_opts(&storage).await;
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
async fn put_get_attributes() {
    let path = tempfile::tempdir().unwrap();
    let storage = FjallStore::open(path.path()).await.unwrap();

    object_store::integration::put_get_attributes(&storage).await;
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
