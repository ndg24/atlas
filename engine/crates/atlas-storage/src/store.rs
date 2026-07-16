//! Object-store abstraction over local filesystem and (later) MinIO/S3, via
//! `object_store::ObjectStore` directly rather than a hand-rolled trait — it
//! already covers both backends uniformly. `get_range` is what lets a future
//! remote backend serve the same selective column reads that
//! `atlas_format::reader` already does against a local `File`.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

/// A local-filesystem-backed store rooted at `root`; paths passed to
/// `put_file`/`get_bytes`/`get_range` are relative to it.
pub fn local_store(root: &Path) -> Result<Arc<dyn ObjectStore>> {
    std::fs::create_dir_all(root)
        .with_context(|| format!("creating object store root {}", root.display()))?;
    let store = LocalFileSystem::new_with_prefix(root)
        .with_context(|| format!("opening local object store at {}", root.display()))?;
    Ok(Arc::new(store))
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("building a current-thread tokio runtime")
        .block_on(fut)
}

/// Upload the bytes at `local_path` to `store` under `remote_path`.
pub fn put_file(store: &dyn ObjectStore, local_path: &Path, remote_path: &str) -> Result<()> {
    let bytes = std::fs::read(local_path)
        .with_context(|| format!("reading {} to upload", local_path.display()))?;
    block_on(store.put(&ObjectPath::from(remote_path), Bytes::from(bytes).into()))
        .with_context(|| format!("uploading to {remote_path}"))?;
    Ok(())
}

/// Read the full bytes of `remote_path` from `store`.
pub fn get_bytes(store: &dyn ObjectStore, remote_path: &str) -> Result<Bytes> {
    block_on(async {
        let result = store.get(&ObjectPath::from(remote_path)).await?;
        result.bytes().await
    })
    .with_context(|| format!("fetching {remote_path}"))
}

/// Read just `range` of `remote_path`'s bytes.
pub fn get_range(store: &dyn ObjectStore, remote_path: &str, range: Range<usize>) -> Result<Bytes> {
    block_on(store.get_range(&ObjectPath::from(remote_path), range))
        .with_context(|| format!("fetching byte range of {remote_path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_bytes_through_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        std::fs::write(&src, b"hello atlas").unwrap();

        let store = local_store(dir.path()).unwrap();
        put_file(store.as_ref(), &src, "uploaded.bin").unwrap();

        let bytes = get_bytes(store.as_ref(), "uploaded.bin").unwrap();
        assert_eq!(bytes.as_ref(), b"hello atlas");
    }

    #[test]
    fn get_range_reads_only_the_requested_slice() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path()).unwrap();
        let src = dir.path().join("range.bin");
        std::fs::write(&src, b"0123456789").unwrap();
        put_file(store.as_ref(), &src, "range.bin").unwrap();

        let slice = get_range(store.as_ref(), "range.bin", 2..5).unwrap();
        assert_eq!(slice.as_ref(), b"234");
    }
}
