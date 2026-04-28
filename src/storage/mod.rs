//! Object-store-backed storage abstraction.
//!
//! Dispatches at construction time between a local filesystem backend and a
//! remote S3 backend based on the URL scheme of the storage root.
//! All manifest I/O flows through `Arc<dyn ObjectStore>` and works against
//! either backend.
//!
//! Supported schemes:
//!   * bare path (e.g. `/var/data/_remote_store`) — `LocalFileSystem`
//!   * `file://<path>` — `LocalFileSystem`
//!   * `s3://<bucket>[/prefix]` — `AmazonS3` (requires AWS credentials in env)
//!
//! AWS credentials for s3:// backends are read from the standard environment
//! variables supported by `object_store::aws::AmazonS3Builder::from_env`
//! (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_SESSION_TOKEN,
//! AWS_ENDPOINT_URL, etc). The `AWS_ENDPOINT_URL` env var is how a MinIO or
//! RustFS dev server is pointed at.

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::Mutex;
use url::Url;

/// Directory name used under `<data_dir>/` for the node-local remote_store root.
/// Exported so orphan cleanup (which otherwise treats unknown top-level data_dir
/// entries as UUID directories) can exclude it.
pub const REMOTE_STORE_DIR_NAME: &str = "_remote_store";

/// Directory name used under `<data_dir>/` for the node-local split cache.
pub const REMOTE_STORE_CACHE_DIR_NAME: &str = "_remote_store_cache";

/// Directory name used under `<data_dir>/` for in-progress split builds.
pub const REMOTE_STORE_STAGING_DIR_NAME: &str = "_remote_store_staging";

/// Magic header prefix written at the start of every packed split bundle.
/// `FSBND` + version byte. Bumping the version byte reserves room for a
/// future format change without needing a separate checksum scheme.
const BUNDLE_MAGIC: &[u8; 6] = b"FSBND\x01";

/// Backend behind a `StorageManager`. Remote backends have no local root.
#[derive(Debug, Clone)]
enum Backend {
    Local {
        root: PathBuf,
        local: Arc<LocalFileSystem>,
    },
    Remote {
        /// The object_store trait object (e.g. `AmazonS3`).
        store: Arc<dyn ObjectStore>,
        /// The original URI (e.g. `s3://bucket/prefix`) — kept for diagnostics.
        uri: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedSplitStatus {
    pub artifact_cached: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteStoreLoadSnapshot {
    pub inflight_bytes: u64,
    pub queue_depth: usize,
}

#[derive(Debug, Clone)]
struct CachedSplitEntry {
    index_uuid: String,
    split_id: String,
    size_bytes: u64,
    last_access_epoch_ms: u64,
}

fn cache_entry_key(index_uuid: &str, split_id: &str) -> String {
    format!("{index_uuid}/{split_id}")
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn dir_size_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }

    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            total += dir_size_bytes(&entry.path());
        }
    }
    total
}

pub struct SplitCachePin {
    key: String,
    pin_counts: Arc<StdMutex<HashMap<String, usize>>>,
}

impl Drop for SplitCachePin {
    fn drop(&mut self) {
        let mut pin_counts = self.pin_counts.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = pin_counts.get_mut(&self.key) {
            if *count <= 1 {
                pin_counts.remove(&self.key);
            } else {
                *count -= 1;
            }
        }
    }
}

pub struct RemoteStoreLoadGuard {
    bytes: u64,
    inflight_bytes: Arc<AtomicU64>,
    queue_depth: Arc<AtomicUsize>,
}

impl Drop for RemoteStoreLoadGuard {
    fn drop(&mut self) {
        self.inflight_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct StorageManager {
    backend: Backend,
    /// Node-local directory for split download/extract cache and for
    /// in-progress split builds staged before upload. Always set.
    local_workdir: PathBuf,
    /// Serializes concurrent `publish_manifest` calls on this process so
    /// two in-process publishers cannot race on the generation pointer.
    /// The fs/object_store backend does not give us compare-and-set on the
    /// pointer object, so we serialize in-process for correctness and leave
    /// cross-process publish to a dedicated indexer role in a later PR.
    publish_lock: Arc<Mutex<()>>,
    /// Per-split async mutex used to single-flight concurrent download+
    /// extract operations against the local cache. Key format:
    /// `"<index_uuid>/<split_id>"`.
    split_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    cache_entries: Arc<StdMutex<HashMap<String, CachedSplitEntry>>>,
    pin_counts: Arc<StdMutex<HashMap<String, usize>>>,
    remote_store_inflight_bytes: Arc<AtomicU64>,
    remote_store_queue_depth: Arc<AtomicUsize>,
}

impl StorageManager {
    /// Construct a StorageManager from a storage URI and a node-local working
    /// directory for split build-staging and cache.
    ///
    /// Accepts bare paths, `file://` URLs, and `s3://bucket[/prefix]` URLs for
    /// the storage URI. `local_workdir` must be a writable local filesystem
    /// path; it's created if missing.
    pub fn new(path: String, local_workdir: PathBuf) -> Result<Self> {
        let backend = match storage_scheme(&path) {
            StorageScheme::Local => {
                let root = normalize_local_root(&path)?;
                std::fs::create_dir_all(&root)
                    .with_context(|| format!("failed to create storage root {:?}", root))?;
                let root = root
                    .canonicalize()
                    .with_context(|| format!("failed to canonicalize storage root {:?}", root))?;
                let local =
                    Arc::new(LocalFileSystem::new_with_prefix(&root).with_context(|| {
                        format!("failed to create object_store backend for {:?}", root)
                    })?);
                Backend::Local { root, local }
            }
            StorageScheme::S3 => {
                let store = build_s3_backend(&path)?;
                Backend::Remote {
                    store,
                    uri: path.clone(),
                }
            }
            StorageScheme::Unsupported(scheme) => {
                anyhow::bail!(
                    "unsupported storage URL scheme '{}'; supported schemes: file, s3, or bare path",
                    scheme
                );
            }
        };

        std::fs::create_dir_all(&local_workdir).with_context(|| {
            format!(
                "failed to create remote_store local workdir {:?}",
                local_workdir
            )
        })?;
        let local_workdir = local_workdir.canonicalize().with_context(|| {
            format!(
                "failed to canonicalize remote_store local workdir {:?}",
                local_workdir
            )
        })?;

        Ok(Self {
            backend,
            local_workdir,
            publish_lock: Arc::new(Mutex::new(())),
            split_locks: Arc::new(Mutex::new(HashMap::new())),
            cache_entries: Arc::new(StdMutex::new(HashMap::new())),
            pin_counts: Arc::new(StdMutex::new(HashMap::new())),
            remote_store_inflight_bytes: Arc::new(AtomicU64::new(0)),
            remote_store_queue_depth: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns the on-disk root when the backend is local, `None` for remote.
    pub fn root(&self) -> Option<&Path> {
        match &self.backend {
            Backend::Local { root, .. } => Some(root.as_path()),
            Backend::Remote { .. } => None,
        }
    }

    /// URI describing the storage backend (e.g. `s3://bucket/prefix` for
    /// remote, canonical local path for local). Useful for log / error
    /// messages so operators can tell at a glance which backend is active.
    pub fn uri(&self) -> String {
        match &self.backend {
            Backend::Local { root, .. } => root.to_string_lossy().into_owned(),
            Backend::Remote { uri, .. } => uri.clone(),
        }
    }

    /// True when the backend is a local filesystem. Callers that still need
    /// direct disk access (for example, the initial remote_store publish path
    /// that builds a Tantivy index in place) gate behind this.
    pub fn is_local(&self) -> bool {
        matches!(&self.backend, Backend::Local { .. })
    }

    /// Construct a StorageManager rooted inside an existing directory
    /// (uses `<parent>/_remote_store` for the storage backend and
    /// `<parent>/_remote_store_cache` for the node-local split cache).
    /// Convenient for tests and for the node startup path that roots the
    /// store under `<data_dir>/_remote_store`.
    pub fn new_in_path(parent: &Path) -> Result<Self> {
        let root = parent.join(REMOTE_STORE_DIR_NAME);
        let workdir = parent.join(REMOTE_STORE_CACHE_DIR_NAME);
        Self::new(root.to_string_lossy().into_owned(), workdir)
    }

    /// Node-local working directory root (cache + staging live under this).
    pub fn local_workdir(&self) -> &Path {
        &self.local_workdir
    }

    /// Cache directory for a specific (index, split). The path is always
    /// computed from trusted IDs (never from untrusted manifest fields).
    fn cache_dir(&self, index_uuid: &str, split_id: &str) -> PathBuf {
        self.local_workdir
            .join("splits")
            .join(index_uuid)
            .join(split_id)
    }

    pub fn cached_split_size_bytes(&self, index_uuid: &str, split_id: &str) -> u64 {
        dir_size_bytes(&self.cache_dir(index_uuid, split_id))
    }

    pub fn record_cached_split_access(&self, index_uuid: &str, split_id: &str, size_bytes: u64) {
        let key = cache_entry_key(index_uuid, split_id);
        let mut cache_entries = self.cache_entries.lock().unwrap_or_else(|e| e.into_inner());
        cache_entries.insert(
            key,
            CachedSplitEntry {
                index_uuid: index_uuid.to_string(),
                split_id: split_id.to_string(),
                size_bytes,
                last_access_epoch_ms: now_epoch_ms(),
            },
        );
    }

    pub fn cached_split_status(
        &self,
        index_uuid: &str,
        split_id: &str,
        checksum: &str,
    ) -> CachedSplitStatus {
        let cache_dir = self.cache_dir(index_uuid, split_id);
        let done_marker = cache_dir.join(".done");
        let artifact_cached = cache_dir.exists()
            && std::fs::read(&done_marker)
                .map(|bytes| bytes == checksum.as_bytes())
                .unwrap_or(false);
        CachedSplitStatus { artifact_cached }
    }

    pub fn pin_cached_split(&self, index_uuid: &str, split_id: &str) -> SplitCachePin {
        let key = cache_entry_key(index_uuid, split_id);
        let mut pin_counts = self.pin_counts.lock().unwrap_or_else(|e| e.into_inner());
        *pin_counts.entry(key.clone()).or_insert(0) += 1;
        SplitCachePin {
            key,
            pin_counts: Arc::clone(&self.pin_counts),
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_split_pin_count(&self, index_uuid: &str, split_id: &str) -> usize {
        let key = cache_entry_key(index_uuid, split_id);
        self.pin_counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .copied()
            .unwrap_or(0)
    }

    pub fn begin_remote_store_batch(&self, bytes: u64) -> RemoteStoreLoadGuard {
        self.remote_store_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        self.remote_store_inflight_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        RemoteStoreLoadGuard {
            bytes,
            inflight_bytes: Arc::clone(&self.remote_store_inflight_bytes),
            queue_depth: Arc::clone(&self.remote_store_queue_depth),
        }
    }

    pub fn remote_store_load_snapshot(&self) -> RemoteStoreLoadSnapshot {
        RemoteStoreLoadSnapshot {
            inflight_bytes: self.remote_store_inflight_bytes.load(Ordering::Relaxed),
            queue_depth: self.remote_store_queue_depth.load(Ordering::Relaxed),
        }
    }

    pub fn reap_split_cache(
        &self,
        index_uuid: &str,
        live_split_ids: &HashSet<String>,
        budget_bytes: Option<u64>,
    ) -> Result<()> {
        let cache_root = self.local_workdir.join("splits").join(index_uuid);

        let mut entries: Vec<(String, CachedSplitEntry)> = {
            let cache_entries = self.cache_entries.lock().unwrap_or_else(|e| e.into_inner());
            cache_entries
                .iter()
                .filter(|(_, entry)| entry.index_uuid == index_uuid)
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect()
        };

        if cache_root.exists() {
            for entry in std::fs::read_dir(&cache_root)
                .with_context(|| format!("read cache root {:?}", cache_root))?
            {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let split_id = entry.file_name().to_string_lossy().to_string();
                let key = cache_entry_key(index_uuid, &split_id);
                if entries.iter().any(|(existing_key, _)| existing_key == &key) {
                    continue;
                }

                entries.push((
                    key,
                    CachedSplitEntry {
                        index_uuid: index_uuid.to_string(),
                        split_id,
                        size_bytes: dir_size_bytes(&entry.path()),
                        last_access_epoch_ms: 0,
                    },
                ));
            }
        }

        let mut total_bytes: u64 = entries.iter().map(|(_, entry)| entry.size_bytes).sum();
        entries.sort_by_key(|(_, entry)| {
            (
                live_split_ids.contains(&entry.split_id),
                entry.last_access_epoch_ms,
            )
        });

        for (key, entry) in entries {
            let is_stale = !live_split_ids.contains(&entry.split_id);
            let over_budget = budget_bytes.is_some_and(|budget| total_bytes > budget);
            if !is_stale && !over_budget {
                continue;
            }

            let is_pinned = self
                .pin_counts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .copied()
                .unwrap_or(0)
                > 0;
            if is_pinned {
                continue;
            }

            let cache_dir = self.cache_dir(index_uuid, &entry.split_id);
            if cache_dir.exists() {
                std::fs::remove_dir_all(&cache_dir)
                    .with_context(|| format!("remove stale cache dir {:?}", cache_dir))?;
            }
            self.cache_entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
            total_bytes = total_bytes.saturating_sub(entry.size_bytes);
        }

        Ok(())
    }

    /// Staging directory for building a split in-progress. Used by the
    /// publish path; the staging tree never appears in the published
    /// storage backend.
    pub fn staging_dir(&self, index_uuid: &str, split_id: &str) -> PathBuf {
        self.local_workdir
            .join("staging")
            .join(index_uuid)
            .join(split_id)
    }

    /// Object-store key where a split's bundle is published. Derived from
    /// trusted IDs so the key is never attacker-controllable.
    pub fn split_bundle_key(index_uuid: &str, split_id: &str) -> String {
        format!("{}/splits/{}/bundle", index_uuid, split_id)
    }

    /// Acquire a per-split async mutex so concurrent downloads of the same
    /// split coalesce into a single fetch.
    async fn split_lock(&self, cache_key: &str) -> Arc<Mutex<()>> {
        let mut map = self.split_locks.lock().await;
        map.entry(cache_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Download a split's bundle from the object store, verify its checksum,
    /// and unpack it into the node-local cache. Reuses the cached copy on
    /// subsequent calls when the `.done` marker matches `expected_checksum`.
    ///
    /// `split` is trusted only for its `split_id`, `bundle_path`, and
    /// `checksum` fields; `bundle_path` is validated against the expected
    /// canonical key shape so a malicious manifest cannot escape the key
    /// namespace or reference another index's splits.
    pub async fn fetch_split_into_cache(
        &self,
        index_uuid: &str,
        split: &RemoteSplitManifest,
    ) -> Result<PathBuf> {
        let expected_key = Self::split_bundle_key(index_uuid, &split.split_id);
        if split.bundle_path != expected_key {
            anyhow::bail!(
                "bundle_path {} does not match canonical key {} for split {}",
                split.bundle_path,
                expected_key,
                split.split_id
            );
        }

        let cache_dir = self.cache_dir(index_uuid, &split.split_id);
        let done_marker = cache_dir.join(".done");
        if let Ok(bytes) = std::fs::read(&done_marker)
            && bytes == split.checksum.as_bytes()
        {
            self.record_cached_split_access(
                index_uuid,
                &split.split_id,
                self.cached_split_size_bytes(index_uuid, &split.split_id),
            );
            return Ok(cache_dir);
        }
        // A stale `.done` with a different checksum means the manifest was
        // republished with a different payload for the same split_id — it
        // shouldn't happen for an immutable split_id, but fall through and
        // re-download defensively.

        let cache_key = format!("{}/{}", index_uuid, split.split_id);
        let guard_arc = self.split_lock(&cache_key).await;
        let _guard = guard_arc.lock().await;

        // Re-check inside the lock: a concurrent caller may have just finished.
        if let Ok(bytes) = std::fs::read(&done_marker)
            && bytes == split.checksum.as_bytes()
        {
            self.record_cached_split_access(
                index_uuid,
                &split.split_id,
                self.cached_split_size_bytes(index_uuid, &split.split_id),
            );
            return Ok(cache_dir);
        }

        // Clean up any partial state from a previous failed extract.
        if cache_dir.exists() {
            std::fs::remove_dir_all(&cache_dir)
                .with_context(|| format!("failed to clear stale cache dir {:?}", cache_dir))?;
        }
        let tmp_dir = cache_dir.with_extension("tmp");
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)
                .with_context(|| format!("failed to clear stale tmp dir {:?}", tmp_dir))?;
        }
        std::fs::create_dir_all(&tmp_dir)
            .with_context(|| format!("failed to create tmp cache dir {:?}", tmp_dir))?;

        let object_path = self.object_path(&split.bundle_path)?;
        let (bundle_bytes, actual_checksum) = self.download_and_hash(&object_path).await?;
        if actual_checksum != split.checksum {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            anyhow::bail!(
                "bundle checksum mismatch for split {}: expected {}, got {}",
                split.split_id,
                split.checksum,
                actual_checksum
            );
        }

        // Unpack in a blocking task so we don't stall the reactor on large
        // bundles.
        let tmp_dir_for_unpack = tmp_dir.clone();
        let unpack_result = tokio::task::spawn_blocking(move || {
            unpack_bundle_into(&bundle_bytes, &tmp_dir_for_unpack)
        })
        .await;

        match unpack_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(e);
            }
            Err(e) => {
                let _ = std::fs::remove_dir_all(&tmp_dir);
                return Err(anyhow::anyhow!("unpack task panicked: {}", e));
            }
        }

        if let Some(parent) = cache_dir.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create cache parent {:?}", parent))?;
        }
        std::fs::rename(&tmp_dir, &cache_dir).with_context(|| {
            format!(
                "failed to promote tmp cache dir {:?} to final {:?}",
                tmp_dir, cache_dir
            )
        })?;
        std::fs::write(&done_marker, split.checksum.as_bytes())
            .with_context(|| format!("failed to write done marker {:?}", done_marker))?;
        self.record_cached_split_access(
            index_uuid,
            &split.split_id,
            self.cached_split_size_bytes(index_uuid, &split.split_id),
        );

        Ok(cache_dir)
    }

    /// Stream-download a split's bundle and compute its sha256 without
    /// writing anything to disk. Used by `verify_splits`.
    pub async fn hash_split_bundle(
        &self,
        index_uuid: &str,
        split: &RemoteSplitManifest,
    ) -> Result<String> {
        let expected_key = Self::split_bundle_key(index_uuid, &split.split_id);
        if split.bundle_path != expected_key {
            anyhow::bail!(
                "bundle_path {} does not match canonical key {} for split {}",
                split.bundle_path,
                expected_key,
                split.split_id
            );
        }
        let object_path = self.object_path(&split.bundle_path)?;
        let mut stream = self.object_store().get(&object_path).await?.into_stream();
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
        }
        Ok(format!("sha256:{:x}", hasher.finalize()))
    }

    /// Upload an already-built local split directory as a packed bundle
    /// to the object store. Returns the `(bundle_key, checksum, size_bytes)`
    /// the caller needs to fill in the manifest entry.
    pub async fn upload_split_bundle(
        &self,
        index_uuid: &str,
        split_id: &str,
        staging_dir: &Path,
    ) -> Result<(String, String, u64)> {
        let staging_dir = staging_dir.to_path_buf();
        let (bundle_bytes, checksum) =
            tokio::task::spawn_blocking(move || pack_bundle_from_dir(&staging_dir))
                .await
                .map_err(|e| anyhow::anyhow!("pack task panicked: {}", e))??;
        let size_bytes = bundle_bytes.len() as u64;

        let bundle_key = Self::split_bundle_key(index_uuid, split_id);
        let object_path = self.object_path(&bundle_key)?;
        self.put(&object_path, bundle_bytes).await?;
        Ok((bundle_key, checksum, size_bytes))
    }

    /// Stream-download an object into a `Vec<u8>` and compute its sha256 in
    /// the same pass. For MVP we buffer the whole bundle in memory; a
    /// future PR can switch to a file-backed staging buffer for very large
    /// bundles.
    async fn download_and_hash(&self, key: &ObjectPath) -> Result<(Vec<u8>, String)> {
        let mut stream = self.object_store().get(key).await?.into_stream();
        let mut buf: Vec<u8> = Vec::new();
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            buf.extend_from_slice(&chunk);
        }
        let checksum = format!("sha256:{:x}", hasher.finalize());
        Ok((buf, checksum))
    }

    /// Upcast the concrete backend to the generic `ObjectStore` trait object.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self.backend {
            Backend::Local { local, .. } => local.clone(),
            Backend::Remote { store, .. } => store.clone(),
        }
    }

    pub fn object_path(&self, location: &str) -> Result<ObjectPath> {
        ObjectPath::parse(location).map_err(anyhow::Error::from)
    }

    /// Resolves an `ObjectPath` to a filesystem path. Fails for remote backends.
    pub fn filesystem_path(&self, location: &ObjectPath) -> Result<PathBuf> {
        match &self.backend {
            Backend::Local { local, .. } => local
                .path_to_filesystem(location)
                .map_err(anyhow::Error::from),
            Backend::Remote { .. } => anyhow::bail!(
                "filesystem_path is only supported for local storage backends; \
                 current backend is remote ({})",
                self.uri()
            ),
        }
    }

    pub async fn put(&self, location: &ObjectPath, payload: impl Into<PutPayload>) -> Result<()> {
        self.object_store()
            .put(location, payload.into())
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    pub async fn get_bytes(&self, location: &ObjectPath) -> Result<Vec<u8>> {
        let result = self.object_store().get(location).await?;
        let bytes = result.bytes().await?;
        Ok(bytes.to_vec())
    }

    // ── Manifest I/O ─────────────────────────────────────────────────
    //
    // The remote-store engine uses a two-layer manifest: a mutable
    // `manifest.current.json` pointer plus immutable `manifests/<gen>.json`
    // generations. See `docs/remote-store-one-pager.md`.

    /// Load the mutable manifest pointer for an index.
    ///
    /// Returns `Ok(None)` when the pointer does not exist (fresh/empty index).
    /// Corrupt JSON or object-store errors surface as `Err`.
    pub async fn load_manifest_pointer(
        &self,
        index_uuid: &str,
    ) -> Result<Option<RemoteManifestPointer>> {
        let location = self.object_path(&manifest_pointer_key(index_uuid))?;
        match self.object_store().get(&location).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let pointer: RemoteManifestPointer = serde_json::from_slice(&bytes)
                    .with_context(|| format!("failed to parse manifest pointer at {}", location))?;
                Ok(Some(pointer))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(anyhow::Error::from(err)),
        }
    }

    /// Load the immutable manifest for a specific generation.
    ///
    /// Fails closed if the manifest's `generation` does not match the
    /// generation we asked for (guards against stale overwrites) or if its
    /// `schema_hash` does not match `expected_schema_hash` when supplied.
    pub async fn load_manifest(
        &self,
        index_uuid: &str,
        generation: u64,
        expected_schema_hash: Option<&str>,
    ) -> Result<RemoteStoreManifest> {
        let location = self.object_path(&manifest_generation_key(index_uuid, generation))?;
        let result = self
            .object_store()
            .get(&location)
            .await
            .with_context(|| format!("manifest generation {} not found", generation))?;
        let bytes = result.bytes().await?;
        let manifest: RemoteStoreManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse manifest at {}", location))?;

        if manifest.generation != generation {
            anyhow::bail!(
                "manifest generation mismatch: pointer requested {}, payload declares {}",
                generation,
                manifest.generation
            );
        }
        if let Some(expected) = expected_schema_hash
            && manifest.schema_hash != expected
        {
            anyhow::bail!(
                "manifest schema_hash mismatch: expected {}, payload declares {}",
                expected,
                manifest.schema_hash
            );
        }
        Ok(manifest)
    }

    /// Convenience: resolve the pointer and load the matching manifest.
    /// Returns `Ok(None)` when the pointer does not exist.
    pub async fn load_current_manifest(
        &self,
        index_uuid: &str,
        expected_schema_hash: Option<&str>,
    ) -> Result<Option<RemoteStoreManifest>> {
        let Some(pointer) = self.load_manifest_pointer(index_uuid).await? else {
            return Ok(None);
        };
        let manifest = self
            .load_manifest(index_uuid, pointer.current_generation, expected_schema_hash)
            .await?;
        Ok(Some(manifest))
    }

    /// Publish a manifest atomically: write the immutable generation first,
    /// then overwrite the mutable pointer. Returns the new pointer.
    ///
    /// If the pointer write fails, the immutable generation object is left in
    /// place (it's immutable by design) and callers can retry.
    ///
    /// Serialized on `publish_lock` so that concurrent in-process publishers
    /// observe a consistent pointer when reading-then-writing.
    pub async fn publish_manifest(
        &self,
        manifest: &RemoteStoreManifest,
    ) -> Result<RemoteManifestPointer> {
        let _guard = self.publish_lock.lock().await;
        self.publish_manifest_inner(manifest).await
    }

    /// Append a single new split to the currently-published manifest and
    /// publish a new generation. Load-modify-publish runs under the per-
    /// storage `publish_lock` so concurrent in-process publishers serialize.
    ///
    /// - If no manifest exists yet, generation 1 is published with the new
    ///   split as the only entry.
    /// - If a manifest exists, its `schema_hash` must match `schema_hash`
    ///   (we fail closed on mismatch to avoid publishing splits against a
    ///   stale schema assumption).
    ///
    /// Returns the newly-published manifest.
    pub async fn append_split_and_publish(
        &self,
        index_uuid: &str,
        index_name: &str,
        schema_hash: &str,
        settings: Option<&crate::cluster::state::RemoteStoreSettings>,
        new_split: RemoteSplitManifest,
    ) -> Result<RemoteStoreManifest> {
        let _guard = self.publish_lock.lock().await;

        // Load current manifest (if any) WITHOUT schema validation — we want
        // to surface a mismatch as a clear, targeted error rather than a
        // generic load failure.
        let current = self.load_current_manifest_unchecked(index_uuid).await?;

        let (generation, mut splits, existing_settings) = match current {
            Some(existing) => {
                if existing.schema_hash != schema_hash {
                    anyhow::bail!(
                        "schema_hash mismatch while appending split: manifest={} local={}",
                        existing.schema_hash,
                        schema_hash
                    );
                }
                (existing.generation + 1, existing.splits, existing.settings)
            }
            None => (1, Vec::new(), None),
        };

        // Reject duplicate split_id within the same manifest — `published_splits`
        // would otherwise return two entries for the same bundle.
        if splits.iter().any(|s| s.split_id == new_split.split_id) {
            anyhow::bail!(
                "split_id {} already exists in manifest for index {}",
                new_split.split_id,
                index_uuid
            );
        }
        splits.push(new_split);

        let manifest = RemoteStoreManifest {
            version: 1,
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            index_uuid: index_uuid.to_string(),
            index_name: index_name.to_string(),
            generation,
            published_at: chrono::Utc::now().to_rfc3339(),
            schema_hash: schema_hash.to_string(),
            settings: settings.cloned().or(existing_settings),
            splits,
        };

        // Reuse `publish_manifest` for the actual I/O — it takes the lock
        // again, but tokio::sync::Mutex is NOT reentrant, so we release
        // ours first by calling the inner non-locking write path directly.
        self.publish_manifest_inner(&manifest).await?;
        Ok(manifest)
    }

    /// Load the current manifest without schema validation. Used by
    /// `append_split_and_publish` so we can produce a better error on
    /// schema mismatch than `load_current_manifest` would.
    async fn load_current_manifest_unchecked(
        &self,
        index_uuid: &str,
    ) -> Result<Option<RemoteStoreManifest>> {
        let Some(pointer) = self.load_manifest_pointer(index_uuid).await? else {
            return Ok(None);
        };
        let manifest = self
            .load_manifest(index_uuid, pointer.current_generation, None)
            .await?;
        Ok(Some(manifest))
    }

    /// Internal publish that does not take the publish_lock (called from
    /// `append_split_and_publish` which already holds it).
    async fn publish_manifest_inner(
        &self,
        manifest: &RemoteStoreManifest,
    ) -> Result<RemoteManifestPointer> {
        let generation_key = manifest_generation_key(&manifest.index_uuid, manifest.generation);
        let generation_path = self.object_path(&generation_key)?;
        let manifest_bytes = serde_json::to_vec_pretty(manifest)
            .context("failed to serialize manifest before publish")?;
        self.put(&generation_path, manifest_bytes).await?;

        let pointer = RemoteManifestPointer {
            version: 1,
            current_generation: manifest.generation,
            manifest_path: generation_key,
            manifest_etag: None,
        };
        let pointer_path = self.object_path(&manifest_pointer_key(&manifest.index_uuid))?;
        let pointer_bytes = serde_json::to_vec_pretty(&pointer)
            .context("failed to serialize manifest pointer before publish")?;
        self.put(&pointer_path, pointer_bytes).await?;
        Ok(pointer)
    }
}

/// Compute a deterministic content fingerprint over an index's field mappings.
///
/// The hash covers `(field_name, field_type, dimension)` for every declared
/// field, sorted by field name. Two mappings that differ in any of those
/// dimensions produce different hashes; two mappings that are logically
/// identical but iterated in different orders produce the same hash.
///
/// Format: `"sha256:<64 hex chars>"` so the prefix identifies the algorithm
/// for future algorithm bumps.
pub fn compute_schema_hash(
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> String {
    // BTreeMap for deterministic iteration order.
    let ordered: BTreeMap<&str, &crate::cluster::state::FieldMapping> =
        mappings.iter().map(|(k, v)| (k.as_str(), v)).collect();

    let mut hasher = Sha256::new();
    for (name, mapping) in ordered {
        hasher.update(name.as_bytes());
        hasher.update(b"\x1f"); // unit separator to avoid name/type bleed
        hasher.update(format!("{:?}", mapping.field_type).as_bytes());
        hasher.update(b"\x1f");
        if let Some(dim) = mapping.dimension {
            hasher.update(dim.to_le_bytes());
        }
        hasher.update(b"\x1e"); // record separator
    }
    let digest = hasher.finalize();
    format!("sha256:{}", hex_lower(&digest))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ── Split bundle format ───────────────────────────────────────────────
//
// A split is uploaded as a single opaque object in the object store.
// The bundle layout is:
//
//   MAGIC:       6 bytes  (b"FSBND\x01")  — format identifier + version
//   file_count:  u64 LE
//   for each file (in POSIX-path sorted order):
//     rel_path_len:    u64 LE
//     rel_path_bytes:  utf8 (never contains "..", never absolute)
//     content_len:     u64 LE
//     content_bytes
//
// The checksum stored in the manifest is the SHA-256 of the entire bundle
// byte stream (`"sha256:<hex>"`). Packing is deterministic (sorted paths +
// explicit length framing) so two bundles with identical contents always
// produce identical bytes and identical checksums, regardless of directory
// iteration order on the builder host.
pub(crate) fn pack_bundle_from_dir(dir: &Path) -> Result<(Vec<u8>, String)> {
    use std::path::Component;

    // Collect every regular file under `dir` with its POSIX relative path.
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries =
            std::fs::read_dir(&next).with_context(|| format!("failed to read_dir {:?}", next))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("dir entry in {:?}", next))?;
            let ft = entry
                .file_type()
                .with_context(|| format!("file_type of {:?}", entry.path()))?;
            let path = entry.path();
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let rel = path
                    .strip_prefix(dir)
                    .with_context(|| format!("path {:?} not under bundle root {:?}", path, dir))?;
                let rel_posix = rel
                    .components()
                    .filter_map(|c| match c {
                        Component::Normal(s) => s.to_str(),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                if rel_posix.is_empty() {
                    anyhow::bail!("empty relative path produced for {:?}", path);
                }
                files.push((rel_posix, path));
            }
            // Skip symlinks and other non-regular entries on purpose.
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(BUNDLE_MAGIC);
    buf.extend_from_slice(&(files.len() as u64).to_le_bytes());
    for (rel_posix, abs_path) in &files {
        let rel_bytes = rel_posix.as_bytes();
        buf.extend_from_slice(&(rel_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(rel_bytes);

        let content = std::fs::read(abs_path)
            .with_context(|| format!("failed to read bundle file {:?}", abs_path))?;
        buf.extend_from_slice(&(content.len() as u64).to_le_bytes());
        buf.extend_from_slice(&content);
    }

    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let checksum = format!("sha256:{:x}", hasher.finalize());
    Ok((buf, checksum))
}

/// Inverse of `pack_bundle_from_dir`: parse `bytes` as a bundle and write
/// each embedded file under `dir`. Rejects malformed frames and any
/// relative path that would escape the target directory.
pub(crate) fn unpack_bundle_into(bytes: &[u8], dir: &Path) -> Result<()> {
    use std::path::Component;

    if bytes.len() < BUNDLE_MAGIC.len() + 8 {
        anyhow::bail!("bundle too short to contain header ({} bytes)", bytes.len());
    }
    if &bytes[..BUNDLE_MAGIC.len()] != BUNDLE_MAGIC {
        anyhow::bail!(
            "bundle magic mismatch: expected {:?}, got {:?}",
            BUNDLE_MAGIC,
            &bytes[..BUNDLE_MAGIC.len()]
        );
    }
    let mut off = BUNDLE_MAGIC.len();

    let file_count = read_u64_le(bytes, &mut off)?;
    // Defensive upper bound: splits realistically have thousands of files
    // at most. Reject values that would let a malformed bundle trigger huge
    // allocations.
    if file_count > 1_000_000 {
        anyhow::bail!(
            "bundle declares implausibly large file_count {}",
            file_count
        );
    }

    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create bundle extract dir {:?}", dir))?;
    let dir_canon = dir
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {:?}", dir))?;

    for _ in 0..file_count {
        let rel_len = read_u64_le(bytes, &mut off)? as usize;
        if off + rel_len > bytes.len() {
            anyhow::bail!("bundle truncated while reading relative path");
        }
        let rel_bytes = &bytes[off..off + rel_len];
        off += rel_len;
        let rel_str =
            std::str::from_utf8(rel_bytes).context("bundle relative path is not valid utf-8")?;

        let rel_path = Path::new(rel_str);
        if rel_path.is_absolute() {
            anyhow::bail!("bundle contains absolute path {:?}", rel_str);
        }
        for component in rel_path.components() {
            match component {
                Component::Normal(_) => {}
                _ => anyhow::bail!("bundle contains disallowed path component in {:?}", rel_str),
            }
        }

        let content_len = read_u64_le(bytes, &mut off)? as usize;
        if off + content_len > bytes.len() {
            anyhow::bail!(
                "bundle truncated while reading file content for {:?}",
                rel_str
            );
        }
        let content = &bytes[off..off + content_len];
        off += content_len;

        let abs = dir_canon.join(rel_path);
        // Belt-and-suspenders: ensure the joined path still lives under `dir_canon`.
        if !abs.starts_with(&dir_canon) {
            anyhow::bail!(
                "bundle file path {:?} would escape extract dir {:?}",
                abs,
                dir_canon
            );
        }
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create parent dir {:?}", parent))?;
        }
        std::fs::write(&abs, content)
            .with_context(|| format!("failed to write bundle file {:?}", abs))?;
    }

    if off != bytes.len() {
        anyhow::bail!(
            "bundle has {} trailing bytes after last file",
            bytes.len() - off
        );
    }
    Ok(())
}

fn read_u64_le(bytes: &[u8], off: &mut usize) -> Result<u64> {
    if *off + 8 > bytes.len() {
        anyhow::bail!("bundle truncated while reading u64");
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[*off..*off + 8]);
    *off += 8;
    Ok(u64::from_le_bytes(b))
}

fn manifest_pointer_key(index_uuid: &str) -> String {
    format!("{}/manifest.current.json", index_uuid)
}

fn manifest_generation_key(index_uuid: &str, generation: u64) -> String {
    // Zero-padded generations keep the object-store listing naturally sorted.
    format!("{}/manifests/{:012}.json", index_uuid, generation)
}

fn normalize_local_root(path: &str) -> Result<PathBuf> {
    if path.starts_with("file://") {
        let url =
            Url::parse(path).with_context(|| format!("invalid file:// storage URL '{}'", path))?;
        url.to_file_path().map_err(|_| {
            anyhow::anyhow!(
                "storage URL '{}' could not be converted to a filesystem path",
                path
            )
        })
    } else {
        Ok(PathBuf::from(path))
    }
}

/// Classification of a storage URI for dispatch in `StorageManager::new`.
enum StorageScheme {
    Local,
    S3,
    Unsupported(String),
}

fn storage_scheme(path: &str) -> StorageScheme {
    if path.starts_with("file://") {
        return StorageScheme::Local;
    }
    if path.starts_with("s3://") {
        return StorageScheme::S3;
    }
    if let Some(scheme_end) = path.find("://") {
        return StorageScheme::Unsupported(path[..scheme_end].to_string());
    }
    StorageScheme::Local
}

/// Build an `AmazonS3` object store from an `s3://bucket[/prefix]` URI.
///
/// Reads credentials and region from environment variables via
/// `AmazonS3Builder::from_env`. `AWS_ENDPOINT_URL` overrides the default
/// AWS endpoint (needed for MinIO / RustFS dev servers).
///
/// When a prefix is present in the URI (e.g. `s3://bucket/indexes`), all
/// manifest keys are scoped under that prefix automatically by the
/// object_store crate's `new_with_prefix` construction.
fn build_s3_backend(uri: &str) -> Result<Arc<dyn ObjectStore>> {
    let url = Url::parse(uri).with_context(|| format!("invalid s3:// storage URL '{}'", uri))?;
    let bucket = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("s3:// URL '{}' is missing a bucket name", uri))?;
    let prefix = url.path().trim_start_matches('/').to_string();

    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

    // `from_env` reads AWS_ENDPOINT_URL, but only if set. For MinIO/RustFS
    // deployments using path-style addressing over plain HTTP, we must also
    // enable virtual_hosted_style=false and allow_http=true. Both are inferred
    // from the endpoint URL scheme when we hand it to the builder explicitly.
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        let allow_http = endpoint.starts_with("http://");
        builder = builder
            .with_endpoint(endpoint)
            .with_virtual_hosted_style_request(false)
            .with_allow_http(allow_http);
    }

    // Region is required by the AWS SDK even for MinIO; default to us-east-1
    // when unset so dev servers work out of the box.
    if std::env::var("AWS_REGION").is_err() && std::env::var("AWS_DEFAULT_REGION").is_err() {
        builder = builder.with_region("us-east-1");
    }

    let s3 = builder
        .build()
        .with_context(|| format!("failed to build S3 backend for '{}'", uri))?;

    if prefix.is_empty() {
        Ok(Arc::new(s3))
    } else {
        // `object_store`'s PrefixStore wraps any ObjectStore with a fixed prefix.
        let prefix_path = ObjectPath::parse(&prefix)
            .with_context(|| format!("invalid s3 prefix '{}' in URL '{}'", prefix, uri))?;
        let prefixed = object_store::prefix::PrefixStore::new(s3, prefix_path);
        Ok(Arc::new(prefixed))
    }
}

/// Mutable pointer to the currently published immutable manifest generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteManifestPointer {
    pub version: u32,
    pub current_generation: u64,
    pub manifest_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_etag: Option<String>,
}

/// Immutable manifest for one remote-store generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteStoreManifest {
    pub version: u32,
    pub engine: crate::cluster::state::IndexEngine,
    pub index_uuid: String,
    pub index_name: String,
    pub generation: u64,
    pub published_at: String,
    pub schema_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<crate::cluster::state::RemoteStoreSettings>,
    #[serde(default)]
    pub splits: Vec<RemoteSplitManifest>,
}

impl RemoteStoreManifest {
    pub fn published_splits(&self) -> impl Iterator<Item = &RemoteSplitManifest> {
        self.splits
            .iter()
            .filter(|split| matches!(split.state, RemoteSplitState::Published))
    }
}

/// State of a split within a remote manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSplitState {
    Staged,
    Published,
    MarkedForDeletion,
}

/// Immutable metadata for one searchable split.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteSplitManifest {
    pub split_id: String,
    pub state: RemoteSplitState,
    pub bundle_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hotcache_path: Option<String>,
    pub checksum: String,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub uncompressed_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hotcache_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<SplitTimeRange>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_ranges: BTreeMap<String, SplitFieldRange>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_terms: BTreeMap<String, SplitFieldTerms>,
}

/// Optional timestamp range summary used for split pruning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitTimeRange {
    pub field: String,
    pub min: String,
    pub max: String,
}

/// Exact min/max summary for a mapped numeric or date field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitFieldRange {
    pub min: String,
    pub max: String,
}

/// Exact small distinct-set summary for a mapped keyword or boolean field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitFieldTerms {
    pub values: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[test]
    fn remote_manifest_pointer_roundtrip() {
        let pointer = RemoteManifestPointer {
            version: 1,
            current_generation: 42,
            manifest_path: "manifests/42.json".into(),
            manifest_etag: Some("etag-42".into()),
        };

        let json = serde_json::to_string(&pointer).unwrap();
        let decoded: RemoteManifestPointer = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, pointer);
    }

    #[test]
    fn remote_store_manifest_roundtrip_preserves_published_splits() {
        let manifest = RemoteStoreManifest {
            version: 1,
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            index_uuid: "idx-uuid".into(),
            index_name: "events".into(),
            generation: 7,
            published_at: "2026-04-16T00:00:00Z".into(),
            schema_hash: "sha256:schema".into(),
            settings: Some(crate::cluster::state::RemoteStoreSettings {
                object_store_uri: Some("s3://bucket/indexes".into()),
                manifest_path: Some("manifests/7.json".into()),
                manifest_generation: Some(7),
                manifest_checksum: Some("sha256:manifest".into()),
                manifest_refresh_ms: Some(1000),
                hotcache_bytes: Some(4096),
                split_cache_bytes: Some(8192),
            }),
            splits: vec![
                RemoteSplitManifest {
                    split_id: "split-a".into(),
                    state: RemoteSplitState::Published,
                    bundle_path: "splits/split-a/bundle.tar.zst".into(),
                    bundle_etag: Some("bundle-a".into()),
                    hotcache_path: Some("splits/split-a/hotcache.bin".into()),
                    checksum: "sha256:split-a".into(),
                    doc_count: 10,
                    size_bytes: 2048,
                    uncompressed_bytes: 4096,
                    hotcache_bytes: Some(256),
                    time_range: Some(SplitTimeRange {
                        field: "created_at".into(),
                        min: "2026-04-01T00:00:00Z".into(),
                        max: "2026-04-02T00:00:00Z".into(),
                    }),
                    tags: BTreeMap::from([("tenant".into(), "blue".into())]),
                    field_ranges: BTreeMap::from([(
                        "created_at".into(),
                        SplitFieldRange {
                            min: "1711929600000".into(),
                            max: "1712016000000".into(),
                        },
                    )]),
                    field_terms: BTreeMap::from([(
                        "status".into(),
                        SplitFieldTerms {
                            values: vec!["error".into(), "warn".into()],
                        },
                    )]),
                },
                RemoteSplitManifest {
                    split_id: "split-b".into(),
                    state: RemoteSplitState::Staged,
                    bundle_path: "splits/split-b/bundle.tar.zst".into(),
                    bundle_etag: None,
                    hotcache_path: None,
                    checksum: "sha256:split-b".into(),
                    doc_count: 3,
                    size_bytes: 1024,
                    uncompressed_bytes: 2048,
                    hotcache_bytes: None,
                    time_range: None,
                    tags: BTreeMap::new(),
                    field_ranges: BTreeMap::new(),
                    field_terms: BTreeMap::new(),
                },
            ],
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: RemoteStoreManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, manifest);

        let published: Vec<_> = decoded
            .published_splits()
            .map(|split| split.split_id.as_str())
            .collect();
        assert_eq!(published, vec!["split-a"]);
    }

    #[tokio::test]
    async fn storage_manager_uses_object_store_local_backend_for_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager.object_path("manifests/1.json").unwrap();

        manager
            .put(&path, "{\"version\":1}".to_string())
            .await
            .unwrap();

        let bytes = manager.get_bytes(&path).await.unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), "{\"version\":1}");
        assert!(manager.filesystem_path(&path).unwrap().exists());
    }

    #[test]
    fn storage_manager_accepts_file_url_root() {
        let temp_dir = TempDir::new().unwrap();
        let root_url = Url::from_file_path(temp_dir.path())
            .expect("temp dir path should convert to a file:// URL");
        let workdir = temp_dir.path().join("workdir");

        let manager = StorageManager::new(root_url.to_string(), workdir).unwrap();

        assert_eq!(
            manager.root().unwrap(),
            temp_dir.path().canonicalize().unwrap()
        );
        assert!(manager.is_local());
    }

    #[test]
    fn storage_manager_rejects_unsupported_scheme() {
        let temp_dir = TempDir::new().unwrap();
        let error = StorageManager::new(
            "gs://bucket/indexes".into(),
            temp_dir.path().join("workdir"),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported storage URL scheme 'gs'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn storage_manager_s3_url_build_requires_env() {
        // When neither AWS_ACCESS_KEY_ID nor an explicit builder is configured
        // and AWS_ENDPOINT_URL is unset, AmazonS3Builder::from_env returns the
        // unconfigured builder which can still build (object_store defers
        // credential resolution until first request). So construction
        // succeeds; we just verify it dispatched to the remote backend and
        // that `filesystem_path` errors instead of returning a bogus path.
        //
        // NOTE: we do NOT set AWS env vars here to avoid racing other tests.
        // The real S3 roundtrip is covered by the env-gated
        // `tests/remote_store_s3_integration.rs` suite.
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new(
            "s3://test-bucket/indexes".into(),
            temp_dir.path().join("workdir"),
        )
        .unwrap();
        assert!(!manager.is_local());
        assert!(manager.root().is_none());
        assert_eq!(manager.uri(), "s3://test-bucket/indexes");

        let path = manager.object_path("foo").unwrap();
        let err = manager.filesystem_path(&path).unwrap_err();
        assert!(
            err.to_string().contains("only supported for local"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn storage_manager_s3_url_rejects_missing_bucket() {
        let temp_dir = TempDir::new().unwrap();
        let err = StorageManager::new("s3:///prefix-only".into(), temp_dir.path().join("workdir"))
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("missing a bucket name") || msg.contains("failed to build S3 backend"),
            "unexpected error: {msg}"
        );
    }

    fn sample_manifest(
        index_uuid: &str,
        generation: u64,
        schema_hash: &str,
    ) -> RemoteStoreManifest {
        RemoteStoreManifest {
            version: 1,
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            index_uuid: index_uuid.into(),
            index_name: "events".into(),
            generation,
            published_at: "2026-04-16T00:00:00Z".into(),
            schema_hash: schema_hash.into(),
            settings: None,
            splits: vec![RemoteSplitManifest {
                split_id: "split-a".into(),
                state: RemoteSplitState::Published,
                bundle_path: "splits/split-a/bundle.tar.zst".into(),
                bundle_etag: None,
                hotcache_path: None,
                checksum: "sha256:a".into(),
                doc_count: 1,
                size_bytes: 16,
                uncompressed_bytes: 32,
                hotcache_bytes: None,
                time_range: None,
                tags: BTreeMap::new(),
                field_ranges: BTreeMap::new(),
                field_terms: BTreeMap::new(),
            }],
        }
    }

    #[tokio::test]
    async fn load_manifest_pointer_returns_none_for_fresh_index() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let pointer = manager.load_manifest_pointer("idx-1").await.unwrap();
        assert!(pointer.is_none());
    }

    #[tokio::test]
    async fn publish_then_load_current_manifest_roundtrips() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let manifest = sample_manifest("idx-1", 7, "sha256:v1");

        let pointer = manager.publish_manifest(&manifest).await.unwrap();
        assert_eq!(pointer.current_generation, 7);
        assert_eq!(pointer.manifest_path, "idx-1/manifests/000000000007.json");

        let loaded_pointer = manager
            .load_manifest_pointer("idx-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded_pointer, pointer);

        let loaded = manager
            .load_current_manifest("idx-1", Some("sha256:v1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, manifest);
    }

    #[tokio::test]
    async fn load_manifest_rejects_schema_hash_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let manifest = sample_manifest("idx-1", 3, "sha256:v1");
        manager.publish_manifest(&manifest).await.unwrap();

        let error = manager
            .load_manifest("idx-1", 3, Some("sha256:different"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("schema_hash mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_manifest_rejects_generation_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        // Publish a manifest whose pointer says generation 5 but whose payload declares 6.
        // We simulate this by writing the manifest by hand under a different generation key.
        let mut manifest = sample_manifest("idx-1", 6, "sha256:v1");
        // Publish generation 6 correctly first, then overwrite the generation-5 object
        // with the generation-6 payload to trigger the mismatch guard.
        manifest.generation = 6;
        let path_5 = manager
            .object_path(&manifest_generation_key("idx-1", 5))
            .unwrap();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        manager.put(&path_5, bytes).await.unwrap();

        let error = manager.load_manifest("idx-1", 5, None).await.unwrap_err();
        assert!(
            error.to_string().contains("manifest generation mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_manifest_rejects_corrupt_json() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager
            .object_path(&manifest_generation_key("idx-1", 1))
            .unwrap();
        manager.put(&path, "not-json".to_string()).await.unwrap();

        let error = manager.load_manifest("idx-1", 1, None).await.unwrap_err();
        assert!(
            error.to_string().contains("failed to parse manifest"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_current_manifest_returns_none_when_pointer_missing() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let loaded = manager.load_current_manifest("idx-1", None).await.unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn compute_schema_hash_is_deterministic_regardless_of_insertion_order() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut a = HashMap::new();
        a.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        a.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let mut b = HashMap::new();
        b.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        b.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        assert_eq!(compute_schema_hash(&a), compute_schema_hash(&b));
    }

    #[test]
    fn compute_schema_hash_differs_on_type_change() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut a = HashMap::new();
        a.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );
        assert_ne!(compute_schema_hash(&a), compute_schema_hash(&b));
    }

    #[test]
    fn compute_schema_hash_differs_on_dimension_change() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut a = HashMap::new();
        a.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(384),
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(768),
            },
        );
        assert_ne!(compute_schema_hash(&a), compute_schema_hash(&b));
    }

    #[test]
    fn compute_schema_hash_is_stable_across_calls() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut m = HashMap::new();
        m.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        let h1 = compute_schema_hash(&m);
        let h2 = compute_schema_hash(&m);
        assert_eq!(h1, h2);
        assert!(h1.starts_with("sha256:"));
        assert_eq!(h1.len(), "sha256:".len() + 64);
    }

    #[tokio::test]
    async fn append_split_and_publish_bootstraps_generation_1() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let manifest = manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split)
            .await
            .unwrap();
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.splits.len(), 1);

        let loaded = manager
            .load_current_manifest("idx-1", Some("sha256:abc"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.generation, 1);
        assert_eq!(loaded.splits[0].split_id, "split-a");
    }

    #[tokio::test]
    async fn append_split_and_publish_bumps_generation_and_appends() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split_a = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split_a)
            .await
            .unwrap();

        let split_b = RemoteSplitManifest {
            split_id: "split-b".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-b".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:b".into(),
            doc_count: 2,
            size_bytes: 32,
            uncompressed_bytes: 32,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let second = manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split_b)
            .await
            .unwrap();

        assert_eq!(second.generation, 2);
        assert_eq!(second.splits.len(), 2);
        assert_eq!(second.splits[0].split_id, "split-a");
        assert_eq!(second.splits[1].split_id, "split-b");
    }

    #[tokio::test]
    async fn append_split_and_publish_rejects_schema_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split.clone())
            .await
            .unwrap();

        let mut split_b = split.clone();
        split_b.split_id = "split-b".into();
        let err = manager
            .append_split_and_publish("idx-1", "events", "sha256:DIFFERENT", None, split_b)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("schema_hash mismatch"));
    }

    #[tokio::test]
    async fn append_split_and_publish_rejects_duplicate_split_id() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split.clone())
            .await
            .unwrap();
        let err = manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // ── Bundle pack/unpack ────────────────────────────────────────────

    fn write_file(root: &Path, rel: &str, content: &[u8]) {
        let abs = root.join(rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(abs, content).unwrap();
    }

    #[test]
    fn pack_bundle_produces_deterministic_sha256() {
        // Same contents, different creation order → identical bytes + checksum.
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        write_file(a.path(), "index/meta.json", b"{\"v\":1}");
        write_file(a.path(), "index/000.store", b"\x00\x01\x02");
        write_file(b.path(), "index/000.store", b"\x00\x01\x02");
        write_file(b.path(), "index/meta.json", b"{\"v\":1}");

        let (bytes_a, sum_a) = pack_bundle_from_dir(a.path()).unwrap();
        let (bytes_b, sum_b) = pack_bundle_from_dir(b.path()).unwrap();
        assert_eq!(bytes_a, bytes_b);
        assert_eq!(sum_a, sum_b);
        assert!(sum_a.starts_with("sha256:"));
        assert_eq!(sum_a.len(), "sha256:".len() + 64);
    }

    #[test]
    fn pack_then_unpack_roundtrip_preserves_every_file() {
        let src = TempDir::new().unwrap();
        write_file(src.path(), "index/meta.json", b"{\"hello\":true}");
        write_file(src.path(), "index/nested/dir/blob.bin", b"\xaa\xbb\xcc\xdd");
        write_file(src.path(), "top.txt", b"root-level");

        let (bytes, checksum) = pack_bundle_from_dir(src.path()).unwrap();
        assert!(checksum.starts_with("sha256:"));

        let dst = TempDir::new().unwrap();
        unpack_bundle_into(&bytes, dst.path()).unwrap();

        assert_eq!(
            std::fs::read(dst.path().join("index/meta.json")).unwrap(),
            b"{\"hello\":true}"
        );
        assert_eq!(
            std::fs::read(dst.path().join("index/nested/dir/blob.bin")).unwrap(),
            b"\xaa\xbb\xcc\xdd"
        );
        assert_eq!(
            std::fs::read(dst.path().join("top.txt")).unwrap(),
            b"root-level"
        );
    }

    #[test]
    fn pack_then_unpack_then_pack_yields_identical_bytes() {
        let src = TempDir::new().unwrap();
        write_file(src.path(), "a.bin", b"hello");
        write_file(src.path(), "b/c.bin", b"world");
        let (bytes1, checksum1) = pack_bundle_from_dir(src.path()).unwrap();

        let mid = TempDir::new().unwrap();
        unpack_bundle_into(&bytes1, mid.path()).unwrap();
        let (bytes2, checksum2) = pack_bundle_from_dir(mid.path()).unwrap();

        assert_eq!(bytes1, bytes2);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn unpack_rejects_missing_magic() {
        let dst = TempDir::new().unwrap();
        // 14+ bytes so the length check passes, but the magic prefix is wrong.
        let bogus: Vec<u8> = b"BOGUS!!!\x00\x00\x00\x00\x00\x00\x00\x00".to_vec();
        let err = unpack_bundle_into(&bogus, dst.path()).unwrap_err();
        assert!(err.to_string().contains("magic"), "{err}");
    }

    #[test]
    fn unpack_rejects_truncated_header() {
        let dst = TempDir::new().unwrap();
        // First 5 bytes of the magic are valid but the header is incomplete.
        let err = unpack_bundle_into(b"FSBND", dst.path()).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn unpack_rejects_absolute_path_in_bundle() {
        // Hand-craft a bundle with one file whose rel path is absolute.
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(BUNDLE_MAGIC);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        let evil = "/etc/passwd";
        bytes.extend_from_slice(&(evil.len() as u64).to_le_bytes());
        bytes.extend_from_slice(evil.as_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let dst = TempDir::new().unwrap();
        let err = unpack_bundle_into(&bytes, dst.path()).unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
    }

    #[test]
    fn unpack_rejects_parent_dir_component() {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(BUNDLE_MAGIC);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        let evil = "../escape.txt";
        bytes.extend_from_slice(&(evil.len() as u64).to_le_bytes());
        bytes.extend_from_slice(evil.as_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let dst = TempDir::new().unwrap();
        let err = unpack_bundle_into(&bytes, dst.path()).unwrap_err();
        assert!(
            err.to_string().contains("disallowed path component"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn upload_and_fetch_split_bundle_roundtrips_via_local_backend() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        // Build a local staging dir that looks like a mini split.
        let stage = manager.staging_dir("idx-uuid", "split-42");
        std::fs::create_dir_all(&stage).unwrap();
        write_file(&stage, "index/meta.json", b"{\"meta\":1}");
        write_file(&stage, "index/000.store", b"\x11\x22\x33");

        let (bundle_key, checksum, size) = manager
            .upload_split_bundle("idx-uuid", "split-42", &stage)
            .await
            .unwrap();
        assert_eq!(bundle_key, "idx-uuid/splits/split-42/bundle");
        assert!(checksum.starts_with("sha256:"));
        assert!(size > 0);

        let split = RemoteSplitManifest {
            split_id: "split-42".into(),
            state: RemoteSplitState::Published,
            bundle_path: bundle_key,
            bundle_etag: None,
            hotcache_path: None,
            checksum: checksum.clone(),
            doc_count: 0,
            size_bytes: size,
            uncompressed_bytes: size,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };

        let hashed = manager.hash_split_bundle("idx-uuid", &split).await.unwrap();
        assert_eq!(hashed, checksum);

        let cache_dir = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(cache_dir.join("index/meta.json")).unwrap(),
            b"{\"meta\":1}"
        );
        assert!(cache_dir.join(".done").exists());
        let cached_size = manager.cached_split_size_bytes("idx-uuid", "split-42");
        assert_eq!(cached_size, dir_size_bytes(&cache_dir));
        assert_ne!(cached_size, size);
        assert_eq!(
            manager
                .cache_entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get("idx-uuid/split-42")
                .map(|entry| entry.size_bytes),
            Some(cached_size)
        );

        // Second fetch should short-circuit without re-downloading (same cache dir).
        let cache_dir_2 = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap();
        assert_eq!(cache_dir, cache_dir_2);
        assert_eq!(
            manager.cached_split_size_bytes("idx-uuid", "split-42"),
            cached_size
        );
    }

    #[tokio::test]
    async fn fetch_split_rejects_unexpected_bundle_path() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let split = RemoteSplitManifest {
            split_id: "split-42".into(),
            state: RemoteSplitState::Published,
            bundle_path: "attacker/splits/split-42/bundle".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:zz".into(),
            doc_count: 0,
            size_bytes: 0,
            uncompressed_bytes: 0,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let err = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not match canonical key"));

        let err = manager
            .hash_split_bundle("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not match canonical key"));
    }

    #[tokio::test]
    async fn fetch_split_rejects_checksum_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let stage = manager.staging_dir("idx-uuid", "split-42");
        std::fs::create_dir_all(&stage).unwrap();
        write_file(&stage, "index/meta.json", b"{}");

        let (bundle_key, checksum, size) = manager
            .upload_split_bundle("idx-uuid", "split-42", &stage)
            .await
            .unwrap();
        let bogus_checksum = format!("sha256:{}", "0".repeat(64));
        assert_ne!(bogus_checksum, checksum);

        let split = RemoteSplitManifest {
            split_id: "split-42".into(),
            state: RemoteSplitState::Published,
            bundle_path: bundle_key,
            bundle_etag: None,
            hotcache_path: None,
            checksum: bogus_checksum,
            doc_count: 0,
            size_bytes: size,
            uncompressed_bytes: size,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let err = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("bundle checksum mismatch"));
    }

    #[test]
    fn remote_store_load_snapshot_tracks_active_batches() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        assert_eq!(manager.remote_store_load_snapshot().inflight_bytes, 0);
        assert_eq!(manager.remote_store_load_snapshot().queue_depth, 0);

        let guard = manager.begin_remote_store_batch(4096);
        let snapshot = manager.remote_store_load_snapshot();
        assert_eq!(snapshot.inflight_bytes, 4096);
        assert_eq!(snapshot.queue_depth, 1);

        {
            let _nested = manager.begin_remote_store_batch(1024);
            let nested_snapshot = manager.remote_store_load_snapshot();
            assert_eq!(nested_snapshot.inflight_bytes, 5120);
            assert_eq!(nested_snapshot.queue_depth, 2);
        }

        let snapshot = manager.remote_store_load_snapshot();
        assert_eq!(snapshot.inflight_bytes, 4096);
        assert_eq!(snapshot.queue_depth, 1);
        drop(guard);

        let snapshot = manager.remote_store_load_snapshot();
        assert_eq!(snapshot.inflight_bytes, 0);
        assert_eq!(snapshot.queue_depth, 0);
    }

    #[test]
    fn reap_split_cache_respects_live_set_budget_and_pins() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        for split_id in ["stale", "live-pinned", "live-budget"] {
            let cache_dir = manager.cache_dir("idx-1", split_id);
            std::fs::create_dir_all(&cache_dir).unwrap();
            std::fs::write(cache_dir.join("blob.bin"), vec![b'x'; 20]).unwrap();
            std::fs::write(cache_dir.join(".done"), b"sha256:test").unwrap();
            manager.record_cached_split_access("idx-1", split_id, dir_size_bytes(&cache_dir));
        }

        {
            let mut entries = manager
                .cache_entries
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            entries.get_mut("idx-1/stale").unwrap().last_access_epoch_ms = 1;
            entries
                .get_mut("idx-1/live-pinned")
                .unwrap()
                .last_access_epoch_ms = 2;
            entries
                .get_mut("idx-1/live-budget")
                .unwrap()
                .last_access_epoch_ms = 3;
        }

        let _pin = manager.pin_cached_split("idx-1", "live-pinned");
        let live_split_ids = HashSet::from(["live-pinned".to_string(), "live-budget".to_string()]);

        manager
            .reap_split_cache("idx-1", &live_split_ids, Some(40))
            .unwrap();

        assert!(!manager.cache_dir("idx-1", "stale").exists());
        assert!(manager.cache_dir("idx-1", "live-pinned").exists());
        assert!(!manager.cache_dir("idx-1", "live-budget").exists());
        assert!(
            manager
                .cached_split_status("idx-1", "live-pinned", "sha256:test")
                .artifact_cached
        );
        assert!(
            !manager
                .cached_split_status("idx-1", "live-budget", "sha256:test")
                .artifact_cached
        );
    }
}
