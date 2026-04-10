use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub const FORCE_MERGE_ACTION: &str = "indices:admin/forcemerge";
const DEFAULT_TASK_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
const DEFAULT_MAX_TERMINAL_LOCAL_TASKS: usize = 4096;
const DEFAULT_MAX_CLUSTER_TASKS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Unknown,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_wire(status: &str) -> Self {
        match status {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
struct LocalForceMergeTask {
    task_id: String,
    node_id: String,
    index_name: String,
    max_num_segments: usize,
    status: TaskStatus,
    created_at_epoch_ms: u64,
    started_at_epoch_ms: Option<u64>,
    completed_at_epoch_ms: Option<u64>,
    successful_shards: u32,
    failed_shards: u32,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct ClusterForceMergeTask {
    task_id: String,
    index_name: String,
    max_num_segments: usize,
    created_at_epoch_ms: u64,
    node_tasks: HashMap<String, String>,
    dispatch_failures: HashMap<String, String>,
}

#[derive(Debug, Clone)]
enum TaskEntry {
    LocalForceMerge(LocalForceMergeTask),
    ClusterForceMerge(ClusterForceMergeTask),
}

#[derive(Clone, Copy)]
struct TaskRetentionPolicy {
    terminal_retention_ms: u64,
    max_terminal_local_tasks: usize,
    max_cluster_tasks: usize,
}

impl Default for TaskRetentionPolicy {
    fn default() -> Self {
        Self {
            terminal_retention_ms: DEFAULT_TASK_RETENTION_MS,
            max_terminal_local_tasks: DEFAULT_MAX_TERMINAL_LOCAL_TASKS,
            max_cluster_tasks: DEFAULT_MAX_CLUSTER_TASKS,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalForceMergeTaskSnapshot {
    pub task_id: String,
    pub action: String,
    pub node_id: String,
    pub index_name: String,
    pub max_num_segments: usize,
    pub status: TaskStatus,
    pub created_at_epoch_ms: u64,
    pub started_at_epoch_ms: Option<u64>,
    pub completed_at_epoch_ms: Option<u64>,
    pub successful_shards: u32,
    pub failed_shards: u32,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterForceMergeTaskSnapshot {
    pub task_id: String,
    pub action: String,
    pub index_name: String,
    pub max_num_segments: usize,
    pub created_at_epoch_ms: u64,
    pub node_tasks: HashMap<String, String>,
    pub dispatch_failures: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum TaskSnapshot {
    LocalForceMerge(LocalForceMergeTaskSnapshot),
    ClusterForceMerge(ClusterForceMergeTaskSnapshot),
}

#[derive(Clone)]
pub struct TaskManager {
    tasks: Arc<RwLock<HashMap<String, TaskEntry>>>,
    retention: TaskRetentionPolicy,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            retention: TaskRetentionPolicy::default(),
        }
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn with_retention_limits(
        terminal_retention_ms: u64,
        max_terminal_local_tasks: usize,
        max_cluster_tasks: usize,
    ) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            retention: TaskRetentionPolicy {
                terminal_retention_ms,
                max_terminal_local_tasks,
                max_cluster_tasks,
            },
        }
    }

    pub fn create_local_force_merge(
        &self,
        node_id: &str,
        index_name: &str,
        max_num_segments: usize,
    ) -> String {
        let task_id = uuid::Uuid::new_v4().simple().to_string();
        let task = LocalForceMergeTask {
            task_id: task_id.clone(),
            node_id: node_id.to_string(),
            index_name: index_name.to_string(),
            max_num_segments,
            status: TaskStatus::Queued,
            created_at_epoch_ms: now_epoch_millis(),
            started_at_epoch_ms: None,
            completed_at_epoch_ms: None,
            successful_shards: 0,
            failed_shards: 0,
            error: None,
        };
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        tasks.insert(task_id.clone(), TaskEntry::LocalForceMerge(task));
        self.prune_tasks(&mut tasks);
        task_id
    }

    pub fn create_cluster_force_merge(
        &self,
        index_name: &str,
        max_num_segments: usize,
        node_tasks: HashMap<String, String>,
        dispatch_failures: HashMap<String, String>,
    ) -> String {
        let task_id = uuid::Uuid::new_v4().simple().to_string();
        let task = ClusterForceMergeTask {
            task_id: task_id.clone(),
            index_name: index_name.to_string(),
            max_num_segments,
            created_at_epoch_ms: now_epoch_millis(),
            node_tasks,
            dispatch_failures,
        };
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        tasks.insert(task_id.clone(), TaskEntry::ClusterForceMerge(task));
        self.prune_tasks(&mut tasks);
        task_id
    }

    pub fn mark_running(&self, task_id: &str) {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        let Some(TaskEntry::LocalForceMerge(task)) = tasks.get_mut(task_id) else {
            return;
        };
        task.status = TaskStatus::Running;
        if task.started_at_epoch_ms.is_none() {
            task.started_at_epoch_ms = Some(now_epoch_millis());
        }
        task.error = None;
        self.prune_tasks(&mut tasks);
    }

    pub fn finish_local_force_merge(
        &self,
        task_id: &str,
        successful_shards: u32,
        failed_shards: u32,
        error: Option<String>,
    ) {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        let Some(TaskEntry::LocalForceMerge(task)) = tasks.get_mut(task_id) else {
            return;
        };

        let finished_at = now_epoch_millis();
        if task.started_at_epoch_ms.is_none() {
            task.started_at_epoch_ms = Some(finished_at);
        }
        task.completed_at_epoch_ms = Some(finished_at);
        task.successful_shards = successful_shards;
        task.failed_shards = failed_shards;
        task.error = error.or_else(|| {
            if failed_shards > 0 {
                Some(format!("{} shard(s) failed", failed_shards))
            } else {
                None
            }
        });
        task.status = if task.error.is_some() {
            TaskStatus::Failed
        } else {
            TaskStatus::Completed
        };
        self.prune_tasks(&mut tasks);
    }

    pub fn fail_local_force_merge(&self, task_id: &str, error: impl Into<String>) {
        self.finish_local_force_merge(task_id, 0, 0, Some(error.into()));
    }

    pub fn get_task(&self, task_id: &str) -> Option<TaskSnapshot> {
        let mut tasks = self.tasks.write().unwrap_or_else(|e| e.into_inner());
        self.prune_tasks(&mut tasks);
        match tasks.get(task_id)? {
            TaskEntry::LocalForceMerge(task) => {
                Some(TaskSnapshot::LocalForceMerge(LocalForceMergeTaskSnapshot {
                    task_id: task.task_id.clone(),
                    action: FORCE_MERGE_ACTION.to_string(),
                    node_id: task.node_id.clone(),
                    index_name: task.index_name.clone(),
                    max_num_segments: task.max_num_segments,
                    status: task.status,
                    created_at_epoch_ms: task.created_at_epoch_ms,
                    started_at_epoch_ms: task.started_at_epoch_ms,
                    completed_at_epoch_ms: task.completed_at_epoch_ms,
                    successful_shards: task.successful_shards,
                    failed_shards: task.failed_shards,
                    error: task.error.clone(),
                }))
            }
            TaskEntry::ClusterForceMerge(task) => Some(TaskSnapshot::ClusterForceMerge(
                ClusterForceMergeTaskSnapshot {
                    task_id: task.task_id.clone(),
                    action: FORCE_MERGE_ACTION.to_string(),
                    index_name: task.index_name.clone(),
                    max_num_segments: task.max_num_segments,
                    created_at_epoch_ms: task.created_at_epoch_ms,
                    node_tasks: task.node_tasks.clone(),
                    dispatch_failures: task.dispatch_failures.clone(),
                },
            )),
        }
    }

    pub fn get_local_force_merge(&self, task_id: &str) -> Option<LocalForceMergeTaskSnapshot> {
        match self.get_task(task_id)? {
            TaskSnapshot::LocalForceMerge(task) => Some(task),
            TaskSnapshot::ClusterForceMerge(_) => None,
        }
    }

    pub fn get_cluster_force_merge(&self, task_id: &str) -> Option<ClusterForceMergeTaskSnapshot> {
        match self.get_task(task_id)? {
            TaskSnapshot::LocalForceMerge(_) => None,
            TaskSnapshot::ClusterForceMerge(task) => Some(task),
        }
    }

    fn prune_tasks(&self, tasks: &mut HashMap<String, TaskEntry>) {
        let retention_cutoff =
            now_epoch_millis().saturating_sub(self.retention.terminal_retention_ms);

        tasks.retain(|_, entry| match entry {
            TaskEntry::LocalForceMerge(task) => {
                !matches!(task.status, TaskStatus::Completed | TaskStatus::Failed)
                    || task
                        .completed_at_epoch_ms
                        .unwrap_or(task.created_at_epoch_ms)
                        >= retention_cutoff
            }
            TaskEntry::ClusterForceMerge(task) => task.created_at_epoch_ms >= retention_cutoff,
        });

        self.prune_oldest_matching(tasks, self.retention.max_terminal_local_tasks, |entry| {
            match entry {
                TaskEntry::LocalForceMerge(task)
                    if matches!(task.status, TaskStatus::Completed | TaskStatus::Failed) =>
                {
                    Some(
                        task.completed_at_epoch_ms
                            .unwrap_or(task.created_at_epoch_ms),
                    )
                }
                _ => None,
            }
        });
        self.prune_oldest_matching(
            tasks,
            self.retention.max_cluster_tasks,
            |entry| match entry {
                TaskEntry::ClusterForceMerge(task) => Some(task.created_at_epoch_ms),
                TaskEntry::LocalForceMerge(_) => None,
            },
        );
    }

    fn prune_oldest_matching<F>(
        &self,
        tasks: &mut HashMap<String, TaskEntry>,
        max_entries: usize,
        mut timestamp_for_entry: F,
    ) where
        F: FnMut(&TaskEntry) -> Option<u64>,
    {
        let mut candidates: Vec<(String, u64)> = tasks
            .iter()
            .filter_map(|(task_id, entry)| {
                timestamp_for_entry(entry).map(|timestamp| (task_id.clone(), timestamp))
            })
            .collect();
        if candidates.len() <= max_entries {
            return;
        }

        let excess = candidates.len() - max_entries;
        candidates.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        for (task_id, _) in candidates.into_iter().take(excess) {
            tasks.remove(&task_id);
        }
    }
}

fn now_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_force_merge_task_tracks_lifecycle() {
        let manager = TaskManager::new();
        let task_id = manager.create_local_force_merge("node-1", "idx", 1);

        let created = manager.get_local_force_merge(&task_id).unwrap();
        assert_eq!(created.status, TaskStatus::Queued);
        assert_eq!(created.index_name, "idx");
        assert_eq!(created.max_num_segments, 1);

        manager.mark_running(&task_id);
        let running = manager.get_local_force_merge(&task_id).unwrap();
        assert_eq!(running.status, TaskStatus::Running);
        assert!(running.started_at_epoch_ms.is_some());

        manager.finish_local_force_merge(&task_id, 4, 0, None);
        let completed = manager.get_local_force_merge(&task_id).unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(completed.successful_shards, 4);
        assert_eq!(completed.failed_shards, 0);
        assert!(completed.completed_at_epoch_ms.is_some());
    }

    #[test]
    fn cluster_force_merge_task_preserves_node_mappings() {
        let manager = TaskManager::new();
        let task_id = manager.create_cluster_force_merge(
            "idx",
            3,
            HashMap::from([
                ("node-1".to_string(), "task-a".to_string()),
                ("node-2".to_string(), "task-b".to_string()),
            ]),
            HashMap::from([("node-3".to_string(), "rpc timeout".to_string())]),
        );

        let TaskSnapshot::ClusterForceMerge(task) = manager.get_task(&task_id).unwrap() else {
            panic!("expected cluster task");
        };
        assert_eq!(task.index_name, "idx");
        assert_eq!(task.max_num_segments, 3);
        assert_eq!(task.node_tasks["node-1"], "task-a");
        assert_eq!(task.dispatch_failures["node-3"], "rpc timeout");
    }

    #[test]
    fn task_manager_prunes_oldest_terminal_local_tasks_when_over_cap() {
        let manager = TaskManager::with_retention_limits(u64::MAX, 1, usize::MAX);

        let first = manager.create_local_force_merge("node-1", "idx", 1);
        manager.finish_local_force_merge(&first, 1, 0, None);

        let second = manager.create_local_force_merge("node-1", "idx", 1);
        manager.finish_local_force_merge(&second, 1, 0, None);

        assert!(manager.get_local_force_merge(&first).is_none());
        assert!(manager.get_local_force_merge(&second).is_some());
    }

    #[test]
    fn task_manager_prunes_oldest_cluster_tasks_when_over_cap() {
        let manager = TaskManager::with_retention_limits(u64::MAX, usize::MAX, 1);

        let first = manager.create_cluster_force_merge(
            "idx",
            1,
            HashMap::from([("node-1".to_string(), "task-a".to_string())]),
            HashMap::new(),
        );
        let second = manager.create_cluster_force_merge(
            "idx",
            1,
            HashMap::from([("node-1".to_string(), "task-b".to_string())]),
            HashMap::new(),
        );

        assert!(manager.get_cluster_force_merge(&first).is_none());
        assert!(manager.get_cluster_force_merge(&second).is_some());
    }
}
