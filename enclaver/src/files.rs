use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures_util::SinkExt;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};

use notify_debouncer_full::{
    new_debouncer, notify, DebounceEventResult, DebouncedEvent, Debouncer, RecommendedCache,
};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{BytesCodec, FramedWrite};
use tokio_vsock::VsockStream;

use crate::constants::FILE_SYNC_PORT;
use crate::json_transport::JsonTransport;

pub const INOTIFY_EVENT_DEBOUNCE_INTERVAL: Duration = Duration::from_secs(1);
pub const SYNC_IO_BUFFER_SIZE: usize = 65536;

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub size: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum WatchKind {
    Regular,
    Symlink,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Watch {
    pub kind: WatchKind,
    pub file: String,
    pub path: PathBuf,
}

impl fmt::Display for Watch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path_str = self.path.to_string_lossy().to_string();
        if matches!(self.kind, WatchKind::Regular) && self.file == path_str {
            write!(f, "{path_str}")
        } else {
            write!(f, "{} -> {path_str}", self.file)
        }
    }
}

pub struct Watcher {
    debouncer: Debouncer<notify::RecommendedWatcher, RecommendedCache>,
    receiver: mpsc::UnboundedReceiver<DebouncedEvent>,

    notifications: VecDeque<Watch>,

    watched_directories: HashMap<PathBuf, HashSet<PathBuf>>,
    watched_paths: HashMap<String, HashSet<Watch>>,
    watches: HashMap<PathBuf, Watch>,

    files_to_add: Vec<String>,
    files_to_remove: Vec<String>,
}

impl Watcher {
    pub fn new(files: &HashSet<String>) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let debouncer = new_debouncer(
            INOTIFY_EVENT_DEBOUNCE_INTERVAL,
            None,
            move |result: DebounceEventResult| match result {
                Ok(events) => {
                    for event in events {
                        if let Err(err) = event_tx.send(event) {
                            error!("file sync watcher error sending event notification: {err}");
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
        .context("setting file sync watcher inotify event debouncer")?;

        Ok(Self {
            debouncer,
            receiver: event_rx,

            notifications: VecDeque::new(),

            watched_directories: HashMap::with_capacity(files.len()),
            watched_paths: HashMap::with_capacity(files.len()),
            watches: HashMap::with_capacity(files.len()),

            files_to_add: files.iter().cloned().collect::<Vec<_>>(),
            files_to_remove: Vec::with_capacity(files.len()),
        })
    }

    pub async fn run(&mut self, sender: mpsc::UnboundedSender<Watch>) {
        'run: loop {
            let mut files_to_remove = self.files_to_remove.drain(..).collect::<HashSet<_>>();
            for file in files_to_remove.drain() {
                self.unwatch(&file);
            }

            let mut files_to_add = self.files_to_add.drain(..).collect::<HashSet<_>>();
            for file in files_to_add.drain() {
                self.watch(&file);
            }

            self.cleanup_watches();

            while let Some(watch) = self.notifications.pop_front() {
                if let Err(err) = sender.send(watch.clone()) {
                    error!(
                        "file sync watcher error notifying about watch event for {}: {}",
                        watch, err,
                    );
                }
            }

            loop {
                tokio::select! {
                    event = self.receiver.recv() => match event {
                        Some(ref event) => {
                            for path in event.paths.iter() {
                                self.handle_event(path, event);
                            }
                        }
                        None => {
                            error!("file sync watcher event notification channel closed");
                            break 'run;
                        }
                    },
                    _ = sleep(INOTIFY_EVENT_DEBOUNCE_INTERVAL) => continue 'run,
                }
            }
        }
    }

    fn watch(&mut self, file: &String) {
        let path = PathBuf::from(file);

        if path.is_symlink() {
            let mut paths = VecDeque::new();
            paths.push_back(path);

            while let Some(path) = paths.pop_front() {
                if path.is_symlink() {
                    if self.add_watch(WatchKind::Symlink, file, &path).is_err() {
                        return;
                    }

                    let mut link_target = match path.parent() {
                        Some(parent) => parent.to_path_buf(),
                        None => PathBuf::from("/"),
                    };

                    let link = path.read_link().unwrap();

                    for component in link.components() {
                        link_target.push(component);
                        paths.push_back(link_target.clone());
                    }
                } else if path.is_dir() {
                    continue;
                } else if path.is_file() {
                    if self.add_watch(WatchKind::Regular, file, &path).is_err() {
                        return;
                    }

                    break;
                } else {
                    warn!(
                        "file path to sync is a symlink to nonexistent or inaccessible location: {}",
                        file
                    );
                    self.files_to_remove.push(file.clone());
                    return;
                }
            }
        } else if path.is_dir() {
            warn!("file path to sync is a directory - skipping: {}", file);
        } else if path.is_file() {
            let _ = self.add_watch(WatchKind::Regular, file, &path);
        } else {
            warn!(
                "file path to sync is nonexistent or inaccessible - skipping: {}",
                file
            );
        }
    }

    fn unwatch(&mut self, file: &String) {
        if let Some(watches) = self.watched_paths.remove(file) {
            for watch in watches.iter() {
                self.remove_watch(watch);
            }
        }
    }

    fn cleanup_watches(&mut self) {
        let mut unwatch = Vec::new();

        for (directory_path, watches) in self.watched_directories.iter() {
            if watches.is_empty() {
                unwatch.push(directory_path.clone());
            }
        }

        for directory_path in unwatch.iter() {
            let directory = directory_path.to_string_lossy().to_string();
            if let Err(err) = self.debouncer.unwatch(&directory) {
                warn!(
                    "file sync watcher error remowing watch for {}: {}",
                    directory, err
                );
            }
            self.watched_directories.remove(directory_path);
        }
    }

    fn add_watch(&mut self, kind: WatchKind, file: &String, path: &Path) -> Result<()> {
        let path = match kind {
            WatchKind::Regular => path.canonicalize().context("canonicalizing file path")?,
            WatchKind::Symlink => path.to_path_buf(),
        };

        let watch = Watch {
            kind: kind.clone(),
            file: file.clone(),
            path: path.clone(),
        };

        let parent_path = path.parent().context("looking up parent directory")?;
        let parent = match kind {
            WatchKind::Regular => parent_path.to_path_buf(),
            WatchKind::Symlink => parent_path
                .canonicalize()
                .context("canonicalizing parent directory")?,
        };

        if let Some(paths) = self.watched_directories.get_mut(&parent) {
            paths.insert(path.clone());
        } else {
            if let Err(err) = self
                .debouncer
                .watch(&parent, notify::RecursiveMode::NonRecursive)
            {
                error!(
                    "file sync watcher error adding watch {} for {}: {}",
                    parent.to_string_lossy(),
                    watch,
                    err
                );
                if matches!(kind, WatchKind::Symlink) {
                    self.files_to_remove.push(file.clone());
                }
                return Err(anyhow!("{err}"));
            }

            let mut paths = HashSet::new();
            paths.insert(path.clone());
            self.watched_directories.insert(parent.to_path_buf(), paths);
        }

        if let Some(watches) = self.watched_paths.get_mut(file) {
            watches.insert(watch.clone());
        } else {
            let mut watches = HashSet::new();
            watches.insert(watch.clone());
            self.watched_paths.insert(file.clone(), watches);
        }

        self.watches.insert(path.clone(), watch.clone());

        if matches!(kind, WatchKind::Regular) {
            self.notifications.push_back(watch);
        }

        Ok(())
    }

    fn remove_watch(&mut self, watch: &Watch) {
        let path = &watch.path;

        let parent = path.parent().unwrap();

        if let Some(paths) = self.watched_directories.get_mut(parent) {
            paths.remove(path);
        }

        self.watches.remove(path);
    }

    fn handle_event(&mut self, path: &PathBuf, event: &DebouncedEvent) {
        let watch = if let Some(watch) = self.watches.get(path) {
            watch.clone()
        } else {
            return;
        };

        let mut notify = false;

        let refresh_watch = match event.kind {
            notify::event::EventKind::Create(_)
            | notify::event::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            ))
            | notify::event::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::To,
            )) => {
                let is_symlink = path.is_symlink();
                if !is_symlink && !path.is_file() {
                    warn!(
                        "file path to sync has become invalid or inaccessible - removing watch for {}",
                        watch.file
                    );
                    self.files_to_remove.push(watch.file.clone());
                    return;
                }
                match watch.kind {
                    WatchKind::Regular => {
                        if is_symlink {
                            true
                        } else {
                            notify = true;
                            false
                        }
                    }
                    WatchKind::Symlink => true,
                }
            }
            notify::event::EventKind::Modify(notify::event::ModifyKind::Data(_)) => {
                match watch.kind {
                    WatchKind::Regular => {
                        notify = true;
                        false
                    }
                    WatchKind::Symlink => true,
                }
            }
            notify::event::EventKind::Remove(_) => match watch.kind {
                WatchKind::Regular => false,
                WatchKind::Symlink => true,
            },
            _ => false,
        };

        if notify {
            self.notifications.push_back(watch.clone());
        }

        if refresh_watch {
            self.files_to_remove.push(watch.file.clone());
            self.files_to_add.push(watch.file.clone());
        }
    }
}

pub async fn sync_watched_file(cid: u32, watch: &Watch) -> Result<()> {
    let mut conn = Box::pin(VsockStream::connect(cid, FILE_SYNC_PORT).await?);
    let mut file = File::open(&watch.path).await?;

    let metadata = file.metadata().await?;
    let meta = Metadata {
        path: watch.file.clone(),
        size: metadata.len(),
    };

    info!("syncing file: {}", watch);

    Metadata::send(&meta, &mut conn).await?;
    if meta.size > 0 {
        let mut writer = FramedWrite::new(conn, BytesCodec::new());
        let mut buffer = [0u8; SYNC_IO_BUFFER_SIZE];
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            writer.send(Bytes::copy_from_slice(&buffer[..n])).await?;
        }
    }

    Ok(())
}
