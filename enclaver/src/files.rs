use anyhow::{Context, Result};
use log::{error, info, warn};
use std::collections::HashMap;
use std::path::Path;

use notify_debouncer_full::{
    new_debouncer, notify, DebounceEventResult, DebouncedEvent, Debouncer, RecommendedCache,
};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_vsock::VsockStream;

use crate::json_transport::JsonTransport;

pub const DIRECTORY_WATCH_DEBOUNCE_INTERVAL: Duration = Duration::from_millis(500);
pub const SYNC_IO_BUFFER_SIZE: usize = 65536;

// --- Wire protocol types ---

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum SyncMessage {
    ChangesetBegin {
        directory: String,
        files: Vec<FileEntry>,
    },
    ChangesetEnd {
        directory: String,
    },
    ChangesetAck {
        directory: String,
        success: bool,
        error: Option<String>,
    },
    InitialSyncComplete,
    InitialSyncAck,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub manifest_path: String,
    pub size: u64,
}

// --- Directory grouping ---

#[derive(Clone, Debug)]
pub struct DirectoryGroup {
    pub directory: String,
    pub files: HashMap<String, String>, // manifest_path -> filename
}

pub fn group_files_by_directory(manifest_files: &[String]) -> HashMap<String, DirectoryGroup> {
    let mut groups: HashMap<String, DirectoryGroup> = HashMap::new();

    for file_path in manifest_files {
        let path = Path::new(file_path);
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_string_lossy().to_string(),
            _ => "/".to_string(),
        };
        let filename = match path.file_name() {
            Some(name) => name.to_string_lossy().to_string(),
            None => {
                warn!("file path to sync has no filename component: {file_path}");
                continue;
            }
        };

        let group = groups
            .entry(parent.clone())
            .or_insert_with(|| DirectoryGroup {
                directory: parent,
                files: HashMap::new(),
            });
        group.files.insert(file_path.clone(), filename);
    }

    groups
}

// --- Directory group watcher ---

pub struct DirectoryGroupWatcher {
    debouncer: Debouncer<notify::RecommendedWatcher, RecommendedCache>,
    receiver: mpsc::UnboundedReceiver<DebouncedEvent>,
    /// Maps canonical watched directory path -> DirectoryGroup
    dir_to_group: HashMap<String, DirectoryGroup>,
}

impl DirectoryGroupWatcher {
    pub fn new(groups: &HashMap<String, DirectoryGroup>) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let debouncer = new_debouncer(
            DIRECTORY_WATCH_DEBOUNCE_INTERVAL,
            None,
            move |result: DebounceEventResult| match result {
                Ok(events) => {
                    for event in events {
                        if let Err(err) = event_tx.send(event) {
                            error!("file sync watcher error sending event: {err}");
                        }
                    }
                }
                Err(errors) => {
                    for err in errors {
                        error!("file sync watcher error: {err}");
                    }
                }
            },
        )
        .context("creating file sync directory watcher")?;

        let mut watcher = Self {
            debouncer,
            receiver: event_rx,
            dir_to_group: HashMap::with_capacity(groups.len()),
        };

        for group in groups.values() {
            if let Err(err) = watcher.watch_directory(group) {
                error!(
                    "file sync watcher: failed to watch directory {}: {err}",
                    group.directory,
                );
            }
        }

        Ok(watcher)
    }

    fn watch_directory(&mut self, group: &DirectoryGroup) -> Result<()> {
        let dir_path = Path::new(&group.directory);

        if !dir_path.is_dir() {
            warn!(
                "file sync watcher: directory does not exist yet, watching anyway: {}",
                group.directory
            );
        }

        // Canonicalize if possible (for matching events), fall back to original path
        let canonical = dir_path
            .canonicalize()
            .unwrap_or_else(|_| dir_path.to_path_buf());

        self.debouncer
            .watch(&canonical, notify::RecursiveMode::NonRecursive)
            .context(format!("watching directory {}", group.directory))?;

        self.dir_to_group
            .insert(canonical.to_string_lossy().to_string(), group.clone());

        info!(
            "file sync watcher: watching directory {} ({} files)",
            group.directory,
            group.files.len()
        );

        Ok(())
    }

    pub async fn run(&mut self, sender: mpsc::UnboundedSender<DirectoryGroup>) {
        // Emit all groups immediately for initial sync
        for group in self.dir_to_group.values() {
            if let Err(err) = sender.send(group.clone()) {
                error!(
                    "file sync watcher: error sending initial group for {}: {err}",
                    group.directory,
                );
            }
        }

        loop {
            tokio::select! {
                event = self.receiver.recv() => match event {
                    Some(ref event) => {
                        self.handle_event(event, &sender);
                    }
                    None => {
                        error!("file sync watcher: event channel closed");
                        break;
                    }
                },
                _ = sleep(DIRECTORY_WATCH_DEBOUNCE_INTERVAL) => {
                    // Periodic wake-up in case events were missed
                    continue;
                }
            }
        }
    }

    fn handle_event(&self, event: &DebouncedEvent, sender: &mpsc::UnboundedSender<DirectoryGroup>) {
        let dominated = matches!(
            event.kind,
            notify::event::EventKind::Create(_)
                | notify::event::EventKind::Modify(notify::event::ModifyKind::Name(
                    notify::event::RenameMode::Both,
                ))
                | notify::event::EventKind::Modify(notify::event::ModifyKind::Name(
                    notify::event::RenameMode::To,
                ))
                | notify::event::EventKind::Modify(notify::event::ModifyKind::Data(_))
        );

        if !dominated {
            return;
        }

        // Find which directory group this event belongs to by checking event paths
        for path in event.paths.iter() {
            let parent = match path.parent() {
                Some(p) => p.to_string_lossy().to_string(),
                None => continue,
            };

            if let Some(group) = self.dir_to_group.get(&parent) {
                info!(
                    "file sync watcher: change detected in {}, syncing {} files",
                    group.directory,
                    group.files.len()
                );
                if let Err(err) = sender.send(group.clone()) {
                    error!(
                        "file sync watcher: error sending group for {}: {err}",
                        group.directory,
                    );
                }
                // Only emit once per event even if multiple paths match the same group
                return;
            }
        }
    }
}

// --- Changeset transfer (host side) ---

pub async fn sync_directory_group(conn: &mut VsockStream, group: &DirectoryGroup) -> Result<()> {
    // Phase 1: resolve all files and collect metadata
    let mut entries = Vec::with_capacity(group.files.len());
    let mut file_paths = Vec::with_capacity(group.files.len());

    for (manifest_path, filename) in &group.files {
        let path = Path::new(manifest_path);

        // Follow symlinks to get actual content
        let resolved = match tokio::fs::canonicalize(path).await {
            Ok(p) => p,
            Err(err) => {
                warn!(
                    "file sync: cannot resolve {manifest_path}, skipping changeset for {}: {err}",
                    group.directory,
                );
                return Ok(());
            }
        };

        let metadata = match tokio::fs::metadata(&resolved).await {
            Ok(m) => m,
            Err(err) => {
                warn!(
                    "file sync: cannot stat {manifest_path}, skipping changeset for {}: {err}",
                    group.directory,
                );
                return Ok(());
            }
        };

        entries.push(FileEntry {
            name: filename.clone(),
            manifest_path: manifest_path.clone(),
            size: metadata.len(),
        });
        file_paths.push(resolved);
    }

    info!(
        "syncing directory group {} ({} files)",
        group.directory,
        entries.len()
    );

    // Phase 2: send ChangesetBegin
    let begin = SyncMessage::ChangesetBegin {
        directory: group.directory.clone(),
        files: entries.clone(),
    };
    SyncMessage::send(&begin, conn).await?;

    // Phase 3: send file contents in order
    for (i, entry) in entries.iter().enumerate() {
        let mut file = File::open(&file_paths[i])
            .await
            .context(format!("opening file {} for sync", entry.manifest_path))?;

        let mut buffer = [0u8; SYNC_IO_BUFFER_SIZE];
        let mut bytes_remaining = entry.size;

        while bytes_remaining > 0 {
            let to_read = std::cmp::min(bytes_remaining as usize, SYNC_IO_BUFFER_SIZE);
            let n = file.read(&mut buffer[..to_read]).await?;
            if n == 0 {
                break;
            }
            conn.write_all(&buffer[..n]).await?;
            bytes_remaining -= n as u64;
        }
    }

    // Phase 4: send ChangesetEnd
    let end = SyncMessage::ChangesetEnd {
        directory: group.directory.clone(),
    };
    SyncMessage::send(&end, conn).await?;

    // Phase 5: wait for ChangesetAck
    let ack: SyncMessage = SyncMessage::recv(conn).await?;
    match ack {
        SyncMessage::ChangesetAck {
            directory: _,
            success: true,
            ..
        } => {
            info!("file sync: changeset for {} acknowledged", group.directory);
            Ok(())
        }
        SyncMessage::ChangesetAck {
            directory: _,
            success: false,
            error,
            ..
        } => {
            let err_msg = error.unwrap_or_else(|| "unknown error".to_string());
            Err(anyhow::anyhow!(
                "file sync: changeset for {} failed: {err_msg}",
                group.directory,
            ))
        }
        other => Err(anyhow::anyhow!(
            "file sync: unexpected message after changeset: {other:?}"
        )),
    }
}

pub async fn send_initial_sync_complete(conn: &mut VsockStream) -> Result<()> {
    SyncMessage::send(&SyncMessage::InitialSyncComplete, conn).await?;

    let ack: SyncMessage = SyncMessage::recv(conn).await?;
    match ack {
        SyncMessage::InitialSyncAck => {
            info!("file sync: initial sync complete acknowledged by enclave");
            Ok(())
        }
        other => Err(anyhow::anyhow!(
            "file sync: unexpected message after InitialSyncComplete: {other:?}"
        )),
    }
}
