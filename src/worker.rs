//! Isolated thread pools for search and write workloads.
//!
//! Mirrors OpenSearch/Quickwit thread pool architecture: search traffic and
//! write traffic run on **dedicated OS-thread pools** so heavy bulk indexing
//! cannot starve search latency and vice versa.
//!
//! Uses dedicated `rayon::ThreadPool` instances (same approach as Quickwit,
//! Meilisearch, DataFusion). Each pool owns a fixed set of OS threads —
//! true physical isolation, not just semaphore-bounded concurrency on a
//! shared pool.

use std::sync::Arc;

/// Dedicated thread pools for CPU-bound engine operations.
///
/// Search and write operations run on physically separate OS threads.
/// A burst of bulk indexing physically cannot consume search threads.
#[derive(Clone)]
pub struct WorkerPools {
    search_pool: Arc<rayon::ThreadPool>,
    write_pool: Arc<rayon::ThreadPool>,
    search_size: usize,
    write_size: usize,
}

impl WorkerPools {
    /// Create worker pools with explicit sizes.
    pub fn new(search_pool_size: usize, write_pool_size: usize) -> Self {
        let search_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(search_pool_size)
            .thread_name(|i| format!("search-{i}"))
            .build()
            .expect("failed to create search thread pool");

        let write_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(write_pool_size)
            .thread_name(|i| format!("write-{i}"))
            .build()
            .expect("failed to create write thread pool");

        Self {
            search_pool: Arc::new(search_pool),
            write_pool: Arc::new(write_pool),
            search_size: search_pool_size,
            write_size: write_pool_size,
        }
    }

    /// Create worker pools with default sizes based on available CPUs.
    /// Search: `cpus * 3 / 2` (matches OpenSearch default).
    /// Write:  `cpus + 1` (matches OpenSearch default).
    pub fn default_for_system() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let search = (cpus * 3 / 2).max(2);
        let write = (cpus + 1).max(2);
        Self::new(search, write)
    }

    /// Run a blocking closure on the **search** thread pool.
    ///
    /// The closure executes on a dedicated OS thread owned by the search pool.
    /// Returns a future that resolves when the closure completes.
    pub async fn spawn_search<F, R>(&self, f: F) -> crate::common::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.search_pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        pool.spawn(move || {
            let result = f();
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| anyhow::anyhow!("search task was cancelled"))
    }

    /// Run a blocking closure on the **write** thread pool.
    ///
    /// The closure executes on a dedicated OS thread owned by the write pool.
    /// Returns a future that resolves when the closure completes.
    pub async fn spawn_write<F, R>(&self, f: F) -> crate::common::Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.write_pool.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        pool.spawn(move || {
            let result = f();
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| anyhow::anyhow!("write task was cancelled"))
    }

    pub fn search_pool_size(&self) -> usize {
        self.search_size
    }

    pub fn write_pool_size(&self) -> usize {
        self.write_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn search_and_write_run_on_separate_pools() {
        let pools = WorkerPools::new(2, 2);

        let (search_result, write_result) = tokio::join!(
            pools.spawn_search(|| {
                let name = std::thread::current().name().unwrap_or("").to_string();
                assert!(
                    name.starts_with("search-"),
                    "expected search thread, got {name}"
                );
                42
            }),
            pools.spawn_write(|| {
                let name = std::thread::current().name().unwrap_or("").to_string();
                assert!(
                    name.starts_with("write-"),
                    "expected write thread, got {name}"
                );
                "written"
            }),
        );

        assert_eq!(search_result.unwrap(), 42);
        assert_eq!(write_result.unwrap(), "written");
    }

    #[tokio::test]
    async fn search_pool_isolates_from_write_pool() {
        let pools = WorkerPools::new(1, 1);
        let pools2 = pools.clone();

        // Block the single write thread
        let write_handle = tokio::spawn(async move {
            pools2
                .spawn_write(|| {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    "slow_write"
                })
                .await
        });

        // Search should still complete immediately on its own thread
        let search_start = std::time::Instant::now();
        let search_result = pools.spawn_search(|| "fast_search").await;
        let search_elapsed = search_start.elapsed();

        assert_eq!(search_result.unwrap(), "fast_search");
        assert!(
            search_elapsed.as_millis() < 50,
            "search took {}ms — should not be blocked by write",
            search_elapsed.as_millis()
        );

        let write_result = write_handle.await.unwrap();
        assert_eq!(write_result.unwrap(), "slow_write");
    }

    #[tokio::test]
    async fn default_pool_sizes_are_reasonable() {
        let pools = WorkerPools::default_for_system();
        assert!(pools.search_pool_size() >= 2);
        assert!(pools.write_pool_size() >= 2);
    }
}
