//! Object-store-backed storage abstraction.
//!
//! Today this wraps a local filesystem backend via the `object_store` crate.
//! Cloud backends (S3, Azure, GCS) can be added by extending `normalize_local_root`
//! and constructing a different `Arc<dyn ObjectStore>` at build time.

use anyhow::{Context, Result};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, local::LocalFileSystem, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone)]
pub struct StorageManager {
    root: PathBuf,
    local: Arc<LocalFileSystem>,
}

impl StorageManager {
    pub fn new(path: String) -> Result<Self> {
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

        Ok(Self { root, local })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Construct a StorageManager rooted inside an existing directory
    /// (uses `<parent>/_remote_store`). Convenient for tests and for the
    /// node startup path that roots the store under `<data_dir>/_remote_store`.
    pub fn new_in_path(parent: &Path) -> Result<Self> {
        let root = parent.join("_remote_store");
        Self::new(root.to_string_lossy().into_owned())
    }

    /// Upcast the concrete local backend to the generic `ObjectStore` trait object.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.local.clone()
    }

    pub fn object_path(&self, location: &str) -> Result<ObjectPath> {
        ObjectPath::parse(location).map_err(anyhow::Error::from)
    }

    pub fn filesystem_path(&self, location: &ObjectPath) -> Result<PathBuf> {
        self.local
            .path_to_filesystem(location)
            .map_err(anyhow::Error::from)
    }

    pub async fn put(&self, location: &ObjectPath, payload: impl Into<PutPayload>) -> Result<()> {
        self.local
            .put(location, payload.into())
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    pub async fn get_bytes(&self, location: &ObjectPath) -> Result<Vec<u8>> {
        let result = self.local.get(location).await?;
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
        match self.local.get(&location).await {
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
            .local
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
    pub async fn publish_manifest(
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
    } else if let Some(scheme_end) = path.find("://") {
        let scheme = &path[..scheme_end];
        anyhow::bail!(
            "unsupported storage URL scheme '{}' ; only file:// roots are implemented currently",
            scheme
        );
    } else {
        Ok(PathBuf::from(path))
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
}

/// Optional timestamp range summary used for split pruning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitTimeRange {
    pub field: String,
    pub min: String,
    pub max: String,
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();
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

        let manager = StorageManager::new(root_url.to_string()).unwrap();

        assert_eq!(manager.root(), temp_dir.path().canonicalize().unwrap());
    }

    #[test]
    fn storage_manager_rejects_non_file_scheme() {
        let error = StorageManager::new("s3://bucket/indexes".into()).unwrap_err();
        assert!(
            error.to_string().contains("unsupported storage URL scheme"),
            "unexpected error: {error}"
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
            }],
        }
    }

    #[tokio::test]
    async fn load_manifest_pointer_returns_none_for_fresh_index() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();

        let pointer = manager.load_manifest_pointer("idx-1").await.unwrap();
        assert!(pointer.is_none());
    }

    #[tokio::test]
    async fn publish_then_load_current_manifest_roundtrips() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();
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
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();
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
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();
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
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();
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
        let manager = StorageManager::new(temp_dir.path().to_string_lossy().into_owned()).unwrap();

        let loaded = manager.load_current_manifest("idx-1", None).await.unwrap();
        assert!(loaded.is_none());
    }
}
