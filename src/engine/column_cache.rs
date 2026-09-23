//! Column cache — lazy-loaded, segment-aware cache for SQL analytics and
//! grouped-partials execution.
//!
//! Caches either pre-built Arrow arrays or grouped-partials decoded full-segment
//! columns, keyed by `(SegmentId, column_name, format)`. Tantivy segments are
//! immutable once committed, so cached data never goes stale — entries are
//! evicted only by size pressure.

use anyhow::{Context, Result};
use datafusion::arrow::array::ArrayRef;
use std::sync::Arc;
use tantivy::index::SegmentId;

const FALLBACK_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CgroupMemoryVersion {
    V1,
    V2,
}

impl CgroupMemoryVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnCacheBudget {
    pub configured_percent: u8,
    pub host_memory_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_version: Option<CgroupMemoryVersion>,
    pub effective_memory_bytes: Option<u64>,
    pub cache_bytes: u64,
    pub diagnostics: Vec<String>,
}

impl ColumnCacheBudget {
    fn disabled() -> Self {
        Self {
            configured_percent: 0,
            host_memory_bytes: None,
            cgroup_limit_bytes: None,
            cgroup_version: None,
            effective_memory_bytes: None,
            cache_bytes: 0,
            diagnostics: Vec::new(),
        }
    }

    pub fn source_label(&self) -> &'static str {
        if self.configured_percent == 0 {
            "disabled"
        } else if let Some(version) = self.cgroup_version
            && self.cgroup_limit_bytes.is_some()
        {
            match version {
                CgroupMemoryVersion::V1 => "cgroup_v1",
                CgroupMemoryVersion::V2 => "cgroup_v2",
            }
        } else if self.host_memory_bytes.is_some() {
            "host"
        } else {
            "fallback"
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct LinuxMemoryPaths {
    meminfo: std::path::PathBuf,
    cgroup: std::path::PathBuf,
    mountinfo: std::path::PathBuf,
}

#[cfg(target_os = "linux")]
impl LinuxMemoryPaths {
    fn procfs() -> Self {
        Self {
            meminfo: "/proc/meminfo".into(),
            cgroup: "/proc/self/cgroup".into(),
            mountinfo: "/proc/self/mountinfo".into(),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct CgroupMembership {
    v2_path: Option<std::path::PathBuf>,
    v1_memory_path: Option<std::path::PathBuf>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CgroupMountKind {
    V1Memory,
    V2,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct CgroupMount {
    root: std::path::PathBuf,
    mount_point: std::path::PathBuf,
    kind: CgroupMountKind,
    namespace_relative_root: bool,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct ControllerLimit {
    version: CgroupMemoryVersion,
    limit_bytes: Option<u64>,
    controls_seen: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum CacheFormat {
    Arrow,
    Grouped,
}

/// Cache key: (segment UUID, column name, cache format).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    segment_id: SegmentId,
    column: String,
    format: CacheFormat,
}

/// Cached full-segment values used by grouped-partials readers.
///
/// Numeric metrics want direct typed access by doc ID, while keyword/string
/// group keys want dictionary ordinals rather than expanded strings.
#[derive(Clone)]
pub(crate) enum GroupedColumnCache {
    F64(Arc<[Option<f64>]>),
    I64(Arc<[Option<i64>]>),
    StrOrds(Arc<[Option<u64>]>),
}

impl GroupedColumnCache {
    fn weight_bytes(&self) -> u64 {
        match self {
            Self::F64(values) => values.len() as u64 * std::mem::size_of::<Option<f64>>() as u64,
            Self::I64(values) => values.len() as u64 * std::mem::size_of::<Option<i64>>() as u64,
            Self::StrOrds(values) => {
                values.len() as u64 * std::mem::size_of::<Option<u64>>() as u64
            }
        }
    }
}

#[derive(Clone)]
enum CacheValue {
    Arrow(ArrayRef),
    Grouped(GroupedColumnCache),
}

/// A lazily-populated, size-bounded column cache backed by `moka`.
///
/// Entries are either:
/// - Arrow `ArrayRef`s covering all docs in a segment for one SQL column, or
/// - grouped-partials decoded full-segment values keyed by doc ID.
///
/// The same shared capacity budget covers both formats.
pub struct ColumnCache {
    inner: moka::sync::Cache<CacheKey, CacheValue>,
    /// Selectivity threshold (0.0–1.0). On a cache miss, the full-segment array
    /// is only built when `matched_docs / max_doc >= threshold`. Below this,
    /// only matching docs are read directly without populating the cache.
    populate_threshold: f64,
}

impl ColumnCache {
    /// Create a new column cache with the given maximum size in bytes
    /// and a populate threshold percentage (0–100).
    /// Pass 0 for max_bytes to create a no-op cache that never stores anything.
    /// Pass 0 for threshold_percent to always eagerly populate on miss.
    pub fn new(max_bytes: u64, populate_threshold_percent: u8) -> Self {
        let inner = moka::sync::Cache::builder()
            .weigher(|_key: &CacheKey, value: &CacheValue| {
                let bytes = match value {
                    CacheValue::Arrow(array) => array.get_array_memory_size() as u64,
                    CacheValue::Grouped(column) => column.weight_bytes(),
                };
                bytes.min(u32::MAX as u64) as u32
            })
            .max_capacity(max_bytes)
            .build();
        let threshold = (populate_threshold_percent.min(100) as f64) / 100.0;
        Self {
            inner,
            populate_threshold: threshold,
        }
    }

    /// Returns true if the matched doc ratio justifies building the full-segment
    /// array on a cache miss. When `matched / total < threshold`, the caller
    /// should use selective reads instead of populating the cache.
    pub fn should_populate(&self, matched_docs: usize, segment_max_doc: u32) -> bool {
        if segment_max_doc == 0 {
            return false;
        }
        let ratio = matched_docs as f64 / segment_max_doc as f64;
        ratio >= self.populate_threshold
    }

    /// Try to get a cached column array for the given segment + column.
    pub fn get(&self, segment_id: SegmentId, column: &str) -> Option<ArrayRef> {
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Arrow,
        };
        match self.inner.get(&key) {
            Some(CacheValue::Arrow(array)) => Some(array),
            _ => None,
        }
    }

    /// Insert a column array into the cache.
    /// Skips insertion if the array is larger than 25% of the cache capacity
    /// (avoids building a full-segment array that would be immediately evicted).
    pub fn insert(&self, segment_id: SegmentId, column: &str, array: ArrayRef) {
        let array_bytes = array.get_array_memory_size() as u64;
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Arrow,
        };
        self.insert_value(key, CacheValue::Arrow(array), array_bytes);
    }

    /// Try to get a grouped-partials decoded full-segment column.
    pub(crate) fn get_grouped(
        &self,
        segment_id: SegmentId,
        column: &str,
    ) -> Option<GroupedColumnCache> {
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Grouped,
        };
        match self.inner.get(&key) {
            Some(CacheValue::Grouped(column)) => Some(column),
            _ => None,
        }
    }

    /// Insert a grouped-partials decoded full-segment column into the cache.
    pub(crate) fn insert_grouped(
        &self,
        segment_id: SegmentId,
        column: &str,
        values: GroupedColumnCache,
    ) {
        let value_bytes = values.weight_bytes();
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Grouped,
        };
        self.insert_value(key, CacheValue::Grouped(values), value_bytes);
    }

    fn insert_value(&self, key: CacheKey, value: CacheValue, value_bytes: u64) {
        let max = self.inner.policy().max_capacity().unwrap_or(0);
        if max > 0 && value_bytes > max / 4 {
            return; // too large — would thrash the cache
        }
        self.inner.insert(key, value);
    }

    /// Maximum cache capacity in bytes.
    pub fn max_capacity(&self) -> u64 {
        self.inner.policy().max_capacity().unwrap_or(0)
    }

    /// Number of entries currently in the cache.
    pub fn entry_count(&self) -> u64 {
        self.inner.run_pending_tasks();
        self.inner.entry_count()
    }

    /// Weighted size of all entries (approximate bytes).
    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }
}

/// Resolve the column-cache budget from host memory and applicable hard cgroup
/// memory limits. Blocking procfs/cgroupfs reads are performed by this function;
/// async callers must run it on a blocking thread.
pub fn resolve_column_cache_budget(percent: u8) -> Result<ColumnCacheBudget> {
    if percent == 0 {
        return Ok(ColumnCacheBudget::disabled());
    }

    #[cfg(target_os = "linux")]
    {
        resolve_linux_column_cache_budget(percent, &LinuxMemoryPaths::procfs())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let configured_percent = percent.min(90);
        let cache_bytes =
            ((FALLBACK_MEMORY_BYTES as u128 * configured_percent as u128) / 100) as u64;
        Ok(ColumnCacheBudget {
            configured_percent,
            host_memory_bytes: None,
            cgroup_limit_bytes: None,
            cgroup_version: None,
            effective_memory_bytes: Some(FALLBACK_MEMORY_BYTES),
            cache_bytes,
            diagnostics: vec![
                "host memory detection is unavailable on this platform; using the 1 GiB fallback"
                    .to_string(),
            ],
        })
    }
}

#[cfg(target_os = "linux")]
fn resolve_linux_column_cache_budget(
    percent: u8,
    paths: &LinuxMemoryPaths,
) -> Result<ColumnCacheBudget> {
    if percent == 0 {
        return Ok(ColumnCacheBudget::disabled());
    }
    let configured_percent = percent.min(90);
    let mut diagnostics = Vec::new();

    let host_memory_bytes =
        match read_optional_text(&paths.meminfo, "host memory", &mut diagnostics)? {
            Some(contents) => Some(parse_memtotal_bytes(&contents)?),
            None => {
                diagnostics.push(
                    "using the 1 GiB memory fallback because /proc/meminfo is unavailable"
                        .to_string(),
                );
                None
            }
        };
    let host_basis = host_memory_bytes.unwrap_or(FALLBACK_MEMORY_BYTES);

    let (cgroup_version, cgroup_limit_bytes) =
        resolve_cgroup_limit(paths, host_basis, &mut diagnostics)?;
    let effective_memory_bytes = cgroup_limit_bytes
        .map(|limit| host_basis.min(limit))
        .unwrap_or(host_basis);
    let cache_bytes = ((effective_memory_bytes as u128 * configured_percent as u128) / 100) as u64;

    Ok(ColumnCacheBudget {
        configured_percent,
        host_memory_bytes,
        cgroup_limit_bytes,
        cgroup_version,
        effective_memory_bytes: Some(effective_memory_bytes),
        cache_bytes,
        diagnostics,
    })
}

#[cfg(target_os = "linux")]
fn read_optional_text(
    path: &std::path::Path,
    description: &str,
    diagnostics: &mut Vec<String>,
) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidData => Err(error)
            .with_context(|| format!("failed to decode authoritative {description} file {path:?}")),
        Err(error) => {
            diagnostics.push(format!(
                "{description} controls at {path:?} are unavailable ({error})"
            ));
            Ok(None)
        }
    }
}

#[cfg(target_os = "linux")]
fn parse_memtotal_bytes(contents: &str) -> Result<u64> {
    let mut memtotal = None;
    for line in contents.lines() {
        let Some(value) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        if memtotal.is_some() {
            anyhow::bail!("/proc/meminfo contains more than one MemTotal entry");
        }
        let mut fields = value.split_whitespace();
        let kib = fields
            .next()
            .context("/proc/meminfo MemTotal is missing its numeric value")?
            .parse::<u64>()
            .context("/proc/meminfo MemTotal is not an unsigned integer")?;
        let unit = fields
            .next()
            .context("/proc/meminfo MemTotal is missing its kB unit")?;
        if unit != "kB" || fields.next().is_some() {
            anyhow::bail!("/proc/meminfo MemTotal must contain exactly '<bytes> kB'");
        }
        memtotal = Some(
            kib.checked_mul(1024)
                .context("/proc/meminfo MemTotal overflows bytes")?,
        );
    }
    memtotal.context("/proc/meminfo is missing MemTotal")
}

#[cfg(target_os = "linux")]
fn resolve_cgroup_limit(
    paths: &LinuxMemoryPaths,
    host_basis: u64,
    diagnostics: &mut Vec<String>,
) -> Result<(Option<CgroupMemoryVersion>, Option<u64>)> {
    let Some(cgroup_contents) =
        read_optional_text(&paths.cgroup, "process cgroup membership", diagnostics)?
    else {
        return Ok((None, None));
    };
    let membership = parse_cgroup_membership(&cgroup_contents)?;
    if membership.v2_path.is_none() && membership.v1_memory_path.is_none() {
        diagnostics.push("no cgroup memory controller is assigned to this process".to_string());
        return Ok((None, None));
    }

    let Some(mountinfo_contents) =
        read_optional_text(&paths.mountinfo, "process mount table", diagnostics)?
    else {
        return Ok((None, None));
    };
    let mounts = parse_mountinfo(&mountinfo_contents)?;

    if let Some(cgroup_path) = membership.v2_path.as_deref() {
        match read_controller_limit(
            CgroupMemoryVersion::V2,
            cgroup_path,
            &mounts,
            host_basis,
            diagnostics,
        )? {
            Some(result) if result.controls_seen => {
                return Ok((Some(result.version), result.limit_bytes));
            }
            _ => {}
        }
    }

    if let Some(cgroup_path) = membership.v1_memory_path.as_deref() {
        match read_controller_limit(
            CgroupMemoryVersion::V1,
            cgroup_path,
            &mounts,
            host_basis,
            diagnostics,
        )? {
            Some(result) if result.controls_seen => {
                return Ok((Some(result.version), result.limit_bytes));
            }
            _ => {}
        }
    }

    diagnostics
        .push("cgroup memory hard-limit controls are unavailable; using host memory".to_string());
    Ok((None, None))
}

#[cfg(target_os = "linux")]
fn parse_cgroup_membership(contents: &str) -> Result<CgroupMembership> {
    let mut membership = CgroupMembership {
        v2_path: None,
        v1_memory_path: None,
    };

    for (line_number, line) in contents.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().unwrap_or_default();
        let controllers = fields.next().with_context(|| {
            format!(
                "malformed /proc/self/cgroup line {}: missing controller list",
                line_number + 1
            )
        })?;
        let path = fields.next().with_context(|| {
            format!(
                "malformed /proc/self/cgroup line {}: missing cgroup path",
                line_number + 1
            )
        })?;
        hierarchy.parse::<u32>().with_context(|| {
            format!(
                "malformed /proc/self/cgroup line {}: invalid hierarchy id",
                line_number + 1
            )
        })?;
        let path = validate_absolute_path(path, "cgroup path")?;

        if hierarchy == "0" && controllers.is_empty() {
            if membership.v2_path.replace(path).is_some() {
                anyhow::bail!("/proc/self/cgroup contains multiple cgroup v2 entries");
            }
        } else if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            match membership.v1_memory_path.as_ref() {
                Some(existing) if existing != &path => {
                    anyhow::bail!("/proc/self/cgroup contains conflicting cgroup v1 memory paths");
                }
                Some(_) => {}
                None => membership.v1_memory_path = Some(path),
            }
        }
    }

    Ok(membership)
}

#[cfg(target_os = "linux")]
fn parse_mountinfo(contents: &str) -> Result<Vec<CgroupMount>> {
    let mut mounts = Vec::new();
    for (line_number, line) in contents.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let separator = fields
            .iter()
            .position(|field| *field == "-")
            .with_context(|| {
                format!(
                    "malformed /proc/self/mountinfo line {}: missing separator",
                    line_number + 1
                )
            })?;
        if separator < 6 || fields.len() < separator + 4 {
            anyhow::bail!(
                "malformed /proc/self/mountinfo line {}: missing required fields",
                line_number + 1
            );
        }

        let filesystem_type = fields[separator + 1];
        let mount_options = fields[5];
        let mount_source = fields[separator + 2];
        let super_options = fields[separator + 3];
        let kind = if filesystem_type == "cgroup2" {
            Some(CgroupMountKind::V2)
        } else if filesystem_type == "cgroup"
            && (comma_list_contains(mount_options, "memory")
                || comma_list_contains(super_options, "memory")
                || mount_source == "memory")
        {
            Some(CgroupMountKind::V1Memory)
        } else {
            None
        };
        let Some(kind) = kind else {
            continue;
        };

        let root = decode_mountinfo_path(fields[3]).with_context(|| {
            format!(
                "malformed /proc/self/mountinfo line {} root",
                line_number + 1
            )
        })?;
        let mount_point = decode_mountinfo_path(fields[4]).with_context(|| {
            format!(
                "malformed /proc/self/mountinfo line {} mount point",
                line_number + 1
            )
        })?;
        let (root, namespace_relative_root) = parse_mount_root(&root)?;
        mounts.push(CgroupMount {
            root,
            mount_point: validate_absolute_path(&mount_point, "cgroup mount point")?,
            kind,
            namespace_relative_root,
        });
    }
    Ok(mounts)
}

#[cfg(target_os = "linux")]
fn comma_list_contains(values: &str, expected: &str) -> bool {
    values.split(',').any(|value| value == expected)
}

#[cfg(target_os = "linux")]
fn decode_mountinfo_path(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 3 >= bytes.len()
            || !bytes[index + 1..=index + 3]
                .iter()
                .all(|byte| matches!(byte, b'0'..=b'7'))
        {
            anyhow::bail!("invalid mountinfo escape in {value:?}");
        }
        let octal = ((bytes[index + 1] - b'0') << 6)
            | ((bytes[index + 2] - b'0') << 3)
            | (bytes[index + 3] - b'0');
        decoded.push(octal);
        index += 4;
    }
    String::from_utf8(decoded).context("mountinfo path is not valid UTF-8")
}

#[cfg(target_os = "linux")]
fn parse_mount_root(value: &str) -> Result<(std::path::PathBuf, bool)> {
    use std::path::Component;

    let path = std::path::Path::new(value);
    if !path.is_absolute() {
        anyhow::bail!("cgroup mount root {value:?} is not absolute");
    }
    let mut namespace_relative = false;
    let mut named_component_seen = false;
    for component in path.components() {
        match component {
            Component::ParentDir if named_component_seen => {
                anyhow::bail!(
                    "cgroup mount root {value:?} mixes namespace parent traversal with named components"
                )
            }
            Component::ParentDir => namespace_relative = true,
            Component::CurDir => {
                anyhow::bail!("cgroup mount root {value:?} contains a current-dir component")
            }
            Component::Normal(_) if namespace_relative => {
                anyhow::bail!(
                    "cgroup mount root {value:?} mixes namespace parent traversal with named components"
                )
            }
            Component::Normal(_) => named_component_seen = true,
            _ => {}
        }
    }
    if namespace_relative {
        Ok(("/".into(), true))
    } else {
        Ok((path.to_path_buf(), false))
    }
}

#[cfg(target_os = "linux")]
fn validate_absolute_path(value: &str, description: &str) -> Result<std::path::PathBuf> {
    use std::path::Component;

    let path = std::path::Path::new(value);
    if !path.is_absolute() {
        anyhow::bail!("{description} {value:?} is not absolute");
    }
    for component in path.components() {
        if matches!(component, Component::ParentDir | Component::CurDir) {
            anyhow::bail!("{description} {value:?} contains traversal components");
        }
    }
    Ok(path.to_path_buf())
}

#[cfg(target_os = "linux")]
fn read_controller_limit(
    version: CgroupMemoryVersion,
    cgroup_path: &std::path::Path,
    mounts: &[CgroupMount],
    host_basis: u64,
    diagnostics: &mut Vec<String>,
) -> Result<Option<ControllerLimit>> {
    let expected_kind = match version {
        CgroupMemoryVersion::V1 => CgroupMountKind::V1Memory,
        CgroupMemoryVersion::V2 => CgroupMountKind::V2,
    };
    let relevant_mounts: Vec<&CgroupMount> = mounts
        .iter()
        .filter(|mount| mount.kind == expected_kind)
        .collect();
    if relevant_mounts.is_empty() {
        diagnostics.push(format!(
            "cgroup {} membership exists but no matching memory-controller mount is visible",
            version.as_str()
        ));
        return Ok(None);
    }

    let matching_mounts: Vec<&CgroupMount> = relevant_mounts
        .iter()
        .filter(|mount| mount.namespace_relative_root || cgroup_path.starts_with(&mount.root))
        .copied()
        .collect();
    if matching_mounts.is_empty() {
        let roots: Vec<String> = relevant_mounts
            .iter()
            .map(|mount| mount.root.display().to_string())
            .collect();
        anyhow::bail!(
            "cgroup {} path {:?} does not map beneath any matching mount root {:?}",
            version.as_str(),
            cgroup_path,
            roots
        );
    }

    let control_name = match version {
        CgroupMemoryVersion::V1 => "memory.limit_in_bytes",
        CgroupMemoryVersion::V2 => "memory.max",
    };
    let mut controls_seen = false;
    let mut minimum_limit = None;
    for mount in matching_mounts {
        let relative = if mount.namespace_relative_root {
            diagnostics.push(format!(
                "cgroup {} mount root is namespace-relative; inspecting only ancestors visible beneath {:?}",
                version.as_str(),
                mount.mount_point
            ));
            cgroup_path
                .strip_prefix("/")
                .context("cgroup namespace-relative path is not absolute")?
        } else {
            cgroup_path.strip_prefix(&mount.root).with_context(|| {
                format!(
                    "failed to map cgroup path {:?} beneath mount root {:?}",
                    cgroup_path, mount.root
                )
            })?
        };
        let current_dir = mount.mount_point.join(relative);
        if !current_dir.starts_with(&mount.mount_point) {
            anyhow::bail!(
                "mapped cgroup directory {:?} escapes mount point {:?}",
                current_dir,
                mount.mount_point
            );
        }

        let mut directory = current_dir;
        loop {
            let control_path = directory.join(control_name);
            match std::fs::read_to_string(&control_path) {
                Ok(contents) => {
                    controls_seen = true;
                    let parsed = match version {
                        CgroupMemoryVersion::V1 => parse_v1_memory_limit(&contents, host_basis)
                            .with_context(|| {
                                format!("malformed cgroup v1 hard limit at {control_path:?}")
                            })?,
                        CgroupMemoryVersion::V2 => {
                            parse_v2_memory_limit(&contents).with_context(|| {
                                format!("malformed cgroup v2 hard limit at {control_path:?}")
                            })?
                        }
                    };
                    if let Some(limit) = parsed {
                        minimum_limit =
                            Some(minimum_limit.map_or(limit, |current: u64| current.min(limit)));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                    return Err(error).with_context(|| {
                        format!("failed to decode authoritative cgroup control {control_path:?}")
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => diagnostics.push(format!(
                    "cgroup control {control_path:?} is unavailable ({error})"
                )),
            }

            if directory == mount.mount_point {
                break;
            }
            let parent = directory.parent().with_context(|| {
                format!(
                    "cgroup ancestor traversal escaped mount point {:?}",
                    mount.mount_point
                )
            })?;
            if !parent.starts_with(&mount.mount_point) {
                anyhow::bail!(
                    "cgroup ancestor traversal from {:?} escaped mount point {:?}",
                    directory,
                    mount.mount_point
                );
            }
            directory = parent.to_path_buf();
        }
    }

    if !controls_seen {
        diagnostics.push(format!(
            "no readable {control_name} controls were found beneath matching cgroup mounts"
        ));
    }

    Ok(Some(ControllerLimit {
        version,
        limit_bytes: minimum_limit.filter(|limit| *limit < host_basis),
        controls_seen,
    }))
}

#[cfg(target_os = "linux")]
fn parse_v2_memory_limit(contents: &str) -> Result<Option<u64>> {
    let value = single_control_value(contents)?;
    if value == "max" {
        return Ok(None);
    }
    Ok(Some(value.parse::<u64>().context(
        "memory.max is neither 'max' nor an unsigned integer",
    )?))
}

#[cfg(target_os = "linux")]
fn parse_v1_memory_limit(contents: &str, host_basis: u64) -> Result<Option<u64>> {
    let value = single_control_value(contents)?;
    if value == "-1" {
        return Ok(None);
    }
    let bytes = value
        .parse::<u64>()
        .context("memory.limit_in_bytes is neither -1 nor an unsigned integer")?;
    const V1_64_BIT_UNLIMITED_FLOOR: u64 = i64::MAX as u64 - 1024 * 1024;
    if bytes >= V1_64_BIT_UNLIMITED_FLOOR || bytes >= host_basis {
        Ok(None)
    } else {
        Ok(Some(bytes))
    }
}

#[cfg(target_os = "linux")]
fn single_control_value(contents: &str) -> Result<&str> {
    let mut values = contents.split_whitespace();
    let value = values.next().context("control file is empty")?;
    if values.next().is_some() {
        anyhow::bail!("control file contains more than one value");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};

    fn make_segment_id() -> SegmentId {
        SegmentId::from_uuid_string("00000000-0000-0001-0000-000000000001").unwrap()
    }

    fn make_segment_id_2() -> SegmentId {
        SegmentId::from_uuid_string("00000000-0000-0002-0000-000000000002").unwrap()
    }

    #[test]
    fn cache_miss_returns_none() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        assert!(cache.get(make_segment_id(), "price").is_none());
    }

    #[test]
    fn cache_hit_after_insert() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let seg_id = make_segment_id();

        cache.insert(seg_id, "price", array.clone());
        let cached = cache.get(seg_id, "price").unwrap();
        assert_eq!(cached.len(), 3);
    }

    #[test]
    fn different_segments_are_independent() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg1 = make_segment_id();
        let seg2 = make_segment_id_2();

        let arr1: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let arr2: ArrayRef = Arc::new(Int64Array::from(vec![30, 40, 50]));

        cache.insert(seg1, "count", arr1);
        cache.insert(seg2, "count", arr2);

        assert_eq!(cache.get(seg1, "count").unwrap().len(), 2);
        assert_eq!(cache.get(seg2, "count").unwrap().len(), 3);
    }

    #[test]
    fn different_columns_same_segment() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg = make_segment_id();

        let prices: ArrayRef = Arc::new(Float64Array::from(vec![9.99, 19.99]));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));

        cache.insert(seg, "price", prices);
        cache.insert(seg, "name", names);
        cache.inner.run_pending_tasks();

        assert_eq!(cache.entry_count(), 2);
        assert!(cache.get(seg, "price").is_some());
        assert!(cache.get(seg, "name").is_some());
    }

    #[test]
    fn grouped_cache_hit_after_insert() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg = make_segment_id();
        let ords: Arc<[Option<u64>]> = vec![Some(3), None, Some(8)].into();

        cache.insert_grouped(seg, "brand", GroupedColumnCache::StrOrds(ords.clone()));

        match cache.get_grouped(seg, "brand") {
            Some(GroupedColumnCache::StrOrds(cached)) => {
                assert_eq!(cached.len(), 3);
                assert_eq!(cached[0], Some(3));
                assert_eq!(cached[1], None);
                assert_eq!(cached[2], Some(8));
            }
            Some(_) => panic!("expected cached string ordinals"),
            None => panic!("expected grouped cache entry"),
        }
    }

    #[test]
    fn arrow_and_grouped_entries_use_distinct_keys() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg = make_segment_id();

        let prices: ArrayRef = Arc::new(Float64Array::from(vec![9.99, 19.99]));
        let numeric: Arc<[Option<f64>]> = vec![Some(9.99), Some(19.99)].into();

        cache.insert(seg, "price", prices);
        cache.insert_grouped(seg, "price", GroupedColumnCache::F64(numeric));
        cache.inner.run_pending_tasks();

        assert!(cache.get(seg, "price").is_some());
        assert!(cache.get_grouped(seg, "price").is_some());
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn zero_size_cache_stores_nothing() {
        let cache = ColumnCache::new(0, 0);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        cache.insert(make_segment_id(), "x", array);
        // moka with max_capacity=0 may still briefly hold entries before eviction
        // but get() should eventually return None
        cache.inner.run_pending_tasks();
        assert!(cache.get(make_segment_id(), "x").is_none());
    }

    #[test]
    fn zero_percent_disables_cache_without_probing_memory() {
        let budget = resolve_column_cache_budget(0).unwrap();
        assert_eq!(budget, ColumnCacheBudget::disabled());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn zero_percent_ignores_missing_fixture_files() {
        let dir = tempfile::tempdir().unwrap();
        let paths = LinuxMemoryPaths {
            meminfo: dir.path().join("missing-meminfo"),
            cgroup: dir.path().join("missing-cgroup"),
            mountinfo: dir.path().join("missing-mountinfo"),
        };

        let budget = resolve_linux_column_cache_budget(0, &paths).unwrap();

        assert_eq!(budget, ColumnCacheBudget::disabled());
    }

    #[test]
    fn configured_percent_is_capped_at_90() {
        let at_100 = resolve_column_cache_budget(100).unwrap();
        let at_90 = resolve_column_cache_budget(90).unwrap();
        assert_eq!(at_100.configured_percent, 90);
        assert_eq!(at_100.cache_bytes, at_90.cache_bytes);
    }

    #[cfg(target_os = "linux")]
    struct LinuxMemoryFixture {
        _dir: tempfile::TempDir,
        paths: LinuxMemoryPaths,
        mount_point: std::path::PathBuf,
    }

    #[cfg(target_os = "linux")]
    impl LinuxMemoryFixture {
        fn new(
            host_memory_bytes: u64,
            cgroup_contents: &str,
            mount_root: &str,
            mount_kind: CgroupMountKind,
        ) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let proc_dir = dir.path().join("proc/self");
            let mount_point = dir.path().join("cgroup");
            std::fs::create_dir_all(&proc_dir).unwrap();
            std::fs::create_dir_all(&mount_point).unwrap();
            let meminfo = dir.path().join("proc/meminfo");
            std::fs::write(
                &meminfo,
                format!("MemTotal:       {} kB\n", host_memory_bytes / 1024),
            )
            .unwrap();
            let cgroup = proc_dir.join("cgroup");
            std::fs::write(&cgroup, cgroup_contents).unwrap();
            let mountinfo = proc_dir.join("mountinfo");
            let (filesystem_type, source, super_options) = match mount_kind {
                CgroupMountKind::V1Memory => ("cgroup", "memory", "rw,memory"),
                CgroupMountKind::V2 => ("cgroup2", "cgroup2", "rw"),
            };
            std::fs::write(
                &mountinfo,
                format!(
                    "29 23 0:26 {mount_root} {} rw,nosuid,nodev,noexec,relatime - {filesystem_type} {source} {super_options}\n",
                    mount_point.display()
                ),
            )
            .unwrap();
            Self {
                _dir: dir,
                paths: LinuxMemoryPaths {
                    meminfo,
                    cgroup,
                    mountinfo,
                },
                mount_point,
            }
        }

        fn write_control(&self, relative_dir: &str, name: &str, value: &str) {
            let directory = self.mount_point.join(relative_dir.trim_start_matches('/'));
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(directory.join(name), value).unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    const HOST_32_GIB: u64 = 32 * 1024 * 1024 * 1024;
    #[cfg(target_os = "linux")]
    const TWO_GIB: u64 = 2 * 1024 * 1024 * 1024;

    #[cfg(target_os = "linux")]
    #[test]
    fn v2_limit_caps_host_memory_for_cache_budget() {
        let fixture = LinuxMemoryFixture::new(
            HOST_32_GIB,
            "0::/containers/app\n",
            "/",
            CgroupMountKind::V2,
        );
        fixture.write_control("containers/app", "memory.max", &TWO_GIB.to_string());
        fixture.write_control("containers", "memory.max", "max");
        fixture.write_control("", "memory.max", "max");

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.host_memory_bytes, Some(HOST_32_GIB));
        assert_eq!(budget.cgroup_version, Some(CgroupMemoryVersion::V2));
        assert_eq!(budget.cgroup_limit_bytes, Some(TWO_GIB));
        assert_eq!(budget.effective_memory_bytes, Some(TWO_GIB));
        assert_eq!(budget.cache_bytes, TWO_GIB / 10);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn v2_tighter_ancestor_limit_wins() {
        let fixture =
            LinuxMemoryFixture::new(HOST_32_GIB, "0::/tenant/job\n", "/", CgroupMountKind::V2);
        fixture.write_control(
            "tenant/job",
            "memory.max",
            &(8 * 1024 * 1024 * 1024u64).to_string(),
        );
        fixture.write_control("tenant", "memory.max", &TWO_GIB.to_string());
        fixture.write_control("", "memory.max", "max");

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_limit_bytes, Some(TWO_GIB));
        assert_eq!(budget.cache_bytes, TWO_GIB / 10);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn nested_mount_root_maps_namespaced_cgroup_path_without_escaping_mount() {
        let fixture = LinuxMemoryFixture::new(
            HOST_32_GIB,
            "0::/delegated/workload\n",
            "/delegated",
            CgroupMountKind::V2,
        );
        fixture.write_control(
            "workload",
            "memory.max",
            &(4 * 1024 * 1024 * 1024u64).to_string(),
        );
        fixture.write_control("", "memory.max", &(3 * 1024 * 1024 * 1024u64).to_string());

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_limit_bytes, Some(3 * 1024 * 1024 * 1024u64));
        assert_eq!(budget.cache_bytes, (3 * 1024 * 1024 * 1024u64) / 10);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cgroup_namespace_parent_root_maps_to_visible_mount_root() {
        let fixture = LinuxMemoryFixture::new(HOST_32_GIB, "0::/\n", "/..", CgroupMountKind::V2);
        fixture.write_control("", "memory.max", &TWO_GIB.to_string());

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_limit_bytes, Some(TWO_GIB));
        assert_eq!(budget.cache_bytes, TWO_GIB / 10);
        assert!(
            budget
                .diagnostics
                .iter()
                .any(|message| message.contains("namespace-relative"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn v2_unlimited_uses_host_memory_and_ignores_memory_high() {
        let fixture = LinuxMemoryFixture::new(HOST_32_GIB, "0::/app\n", "/", CgroupMountKind::V2);
        fixture.write_control("app", "memory.max", "max");
        fixture.write_control("app", "memory.high", &TWO_GIB.to_string());
        fixture.write_control("", "memory.max", "max");

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_version, Some(CgroupMemoryVersion::V2));
        assert_eq!(budget.cgroup_limit_bytes, None);
        assert_eq!(budget.effective_memory_bytes, Some(HOST_32_GIB));
        assert_eq!(budget.cache_bytes, HOST_32_GIB / 10);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn v1_memory_controller_limit_caps_host_memory() {
        let fixture = LinuxMemoryFixture::new(
            HOST_32_GIB,
            "5:cpu,memory:/docker/child\n",
            "/docker",
            CgroupMountKind::V1Memory,
        );
        fixture.write_control(
            "child",
            "memory.limit_in_bytes",
            &(6 * 1024 * 1024 * 1024u64).to_string(),
        );
        fixture.write_control("", "memory.limit_in_bytes", &TWO_GIB.to_string());

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_version, Some(CgroupMemoryVersion::V1));
        assert_eq!(budget.cgroup_limit_bytes, Some(TWO_GIB));
        assert_eq!(budget.cache_bytes, TWO_GIB / 10);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn v1_unlimited_values_do_not_cap_host_memory() {
        let fixture = LinuxMemoryFixture::new(
            HOST_32_GIB,
            "5:memory:/docker/child\n",
            "/",
            CgroupMountKind::V1Memory,
        );
        fixture.write_control(
            "docker/child",
            "memory.limit_in_bytes",
            "9223372036854771712",
        );
        fixture.write_control("docker", "memory.limit_in_bytes", "-1");

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_version, Some(CgroupMemoryVersion::V1));
        assert_eq!(budget.cgroup_limit_bytes, None);
        assert_eq!(budget.effective_memory_bytes, Some(HOST_32_GIB));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn no_memory_controller_uses_host_memory_with_diagnostic() {
        let fixture =
            LinuxMemoryFixture::new(HOST_32_GIB, "4:cpu:/app\n", "/", CgroupMountKind::V2);

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_version, None);
        assert_eq!(budget.effective_memory_bytes, Some(HOST_32_GIB));
        assert!(
            budget
                .diagnostics
                .iter()
                .any(|message| message.contains("no cgroup memory controller"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn missing_controller_files_use_host_memory_with_diagnostic() {
        let fixture = LinuxMemoryFixture::new(HOST_32_GIB, "0::/app\n", "/", CgroupMountKind::V2);

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.cgroup_version, None);
        assert_eq!(budget.effective_memory_bytes, Some(HOST_32_GIB));
        assert!(
            budget
                .diagnostics
                .iter()
                .any(|message| message.contains("no readable memory.max"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn malformed_authoritative_limit_is_an_error() {
        let fixture = LinuxMemoryFixture::new(HOST_32_GIB, "0::/app\n", "/", CgroupMountKind::V2);
        fixture.write_control("app", "memory.max", "not-a-limit");

        let error = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap_err();

        assert!(error.to_string().contains("malformed cgroup v2 hard limit"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unmappable_cgroup_path_is_an_error() {
        let fixture = LinuxMemoryFixture::new(
            HOST_32_GIB,
            "0::/tenant/app\n",
            "/different-root",
            CgroupMountKind::V2,
        );

        let error = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap_err();

        assert!(error.to_string().contains("does not map beneath"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn malformed_proc_data_is_an_error() {
        let fixture = LinuxMemoryFixture::new(HOST_32_GIB, "0::/app\n", "/", CgroupMountKind::V2);
        std::fs::write(&fixture.paths.meminfo, "MemTotal: nope kB\n").unwrap();
        let meminfo_error = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap_err();
        assert!(meminfo_error.to_string().contains("MemTotal"));

        std::fs::write(
            &fixture.paths.meminfo,
            format!("MemTotal: {} kB\n", HOST_32_GIB / 1024),
        )
        .unwrap();
        std::fs::write(&fixture.paths.mountinfo, "missing separator\n").unwrap();
        let mountinfo_error = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap_err();
        assert!(mountinfo_error.to_string().contains("missing separator"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mount_root_cannot_mix_named_components_and_parent_traversal() {
        let fixture = LinuxMemoryFixture::new(
            HOST_32_GIB,
            "0::/tenant/app\n",
            "/tenant/..",
            CgroupMountKind::V2,
        );

        let error = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("mixes namespace parent traversal")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unavailable_meminfo_uses_explicit_fallback() {
        let fixture =
            LinuxMemoryFixture::new(HOST_32_GIB, "4:cpu:/app\n", "/", CgroupMountKind::V2);
        std::fs::remove_file(&fixture.paths.meminfo).unwrap();

        let budget = resolve_linux_column_cache_budget(10, &fixture.paths).unwrap();

        assert_eq!(budget.host_memory_bytes, None);
        assert_eq!(budget.effective_memory_bytes, Some(FALLBACK_MEMORY_BYTES));
        assert_eq!(budget.cache_bytes, FALLBACK_MEMORY_BYTES / 10);
        assert_eq!(budget.source_label(), "fallback");
        assert!(
            budget
                .diagnostics
                .iter()
                .any(|message| message.contains("1 GiB memory fallback"))
        );
    }

    // ── Selectivity threshold tests ─────────────────────────────────────

    #[test]
    fn should_populate_zero_threshold_always_true() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        // threshold=0 → always populate (even for 1 match in 1M docs)
        assert!(cache.should_populate(1, 1_000_000));
        assert!(cache.should_populate(0, 1_000_000)); // 0 matches → ratio 0.0 >= 0.0 → true
    }

    #[test]
    fn should_populate_100_threshold_never_populates() {
        let cache = ColumnCache::new(1024 * 1024, 100);
        // threshold=100% → only populate when ALL docs match
        assert!(!cache.should_populate(999, 1000));
        assert!(cache.should_populate(1000, 1000)); // exactly 100%
    }

    #[test]
    fn should_populate_5_percent_threshold() {
        let cache = ColumnCache::new(1024 * 1024, 5);
        // 5% of 100,000 = 5,000
        assert!(!cache.should_populate(4999, 100_000)); // below threshold
        assert!(cache.should_populate(5000, 100_000)); // at threshold
        assert!(cache.should_populate(10000, 100_000)); // above threshold
    }

    #[test]
    fn should_populate_zero_max_doc_returns_false() {
        let cache = ColumnCache::new(1024 * 1024, 5);
        assert!(!cache.should_populate(0, 0));
    }

    #[test]
    fn should_populate_default_threshold() {
        // Default threshold is 5%
        let cache = ColumnCache::new(1024 * 1024, 5);
        // 560K docs, 20 matches → 0.003% → below 5%
        assert!(!cache.should_populate(20, 560_000));
        // 560K docs, 28K matches → 5% → at threshold
        assert!(cache.should_populate(28_000, 560_000));
        // 560K docs, 560K matches → 100% → above threshold
        assert!(cache.should_populate(560_000, 560_000));
    }
}
