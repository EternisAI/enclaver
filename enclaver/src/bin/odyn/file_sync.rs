use anyhow::{anyhow, Context, Result};
use futures::{Stream, StreamExt};
use log::{error, info, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_vsock::VsockStream;

use crate::config::Configuration;
use enclaver::constants::FILE_SYNC_PORT;
use enclaver::files::{self, FileEntry, SyncMessage};
use enclaver::json_transport::JsonTransport;
use enclaver::vsock;

struct DirectoryState {
    /// Name of the current timestamped directory (just the dir name, not full path)
    current_ts_dir: Option<String>,
    /// Whether per-file symlinks have been created for this directory
    symlinks_created: bool,
}

struct FileSyncServer {
    incoming: Box<dyn Stream<Item = VsockStream> + Unpin + Send>,
    initial_sync_notifier: Option<oneshot::Sender<()>>,
    dir_state: HashMap<String, DirectoryState>,
}

impl FileSyncServer {
    pub fn new(sender: oneshot::Sender<()>) -> Result<Self> {
        let incoming = Box::new(vsock::serve(FILE_SYNC_PORT)?);

        Ok(Self {
            incoming,
            initial_sync_notifier: Some(sender),
            dir_state: HashMap::new(),
        })
    }

    pub async fn serve(mut self) -> Result<()> {
        info!("Waiting for the host to connect for file sync");

        let mut incoming = Box::into_pin(self.incoming);

        // Take the stream out so we can borrow self mutably in the loop
        loop {
            // Accept a connection
            let conn = loop {
                match incoming.next().await {
                    Some(stream) => break stream,
                    None => continue,
                }
            };

            info!("File sync: host connected");

            if let Err(err) =
                Self::handle_connection(&mut self.initial_sync_notifier, &mut self.dir_state, conn)
                    .await
            {
                warn!("File sync: connection ended: {err}");
                // On disconnect, accept a new connection (host will re-send all groups)
                info!("File sync: waiting for host to reconnect");
            }
        }
    }

    async fn handle_connection(
        initial_sync_notifier: &mut Option<oneshot::Sender<()>>,
        dir_state: &mut HashMap<String, DirectoryState>,
        mut conn: VsockStream,
    ) -> Result<()> {
        loop {
            let msg: SyncMessage = SyncMessage::recv(&mut conn).await?;

            match msg {
                SyncMessage::ChangesetBegin { directory, files } => {
                    let result =
                        Self::handle_changeset(dir_state, &mut conn, &directory, &files).await;

                    let ack = match result {
                        Ok(()) => SyncMessage::ChangesetAck {
                            directory: directory.clone(),
                            success: true,
                            error: None,
                        },
                        Err(ref err) => {
                            error!("File sync: changeset for {directory} failed: {err}");
                            SyncMessage::ChangesetAck {
                                directory: directory.clone(),
                                success: false,
                                error: Some(format!("{err}")),
                            }
                        }
                    };

                    SyncMessage::send(&ack, &mut conn).await?;
                }
                SyncMessage::InitialSyncComplete => {
                    info!("File sync: initial sync complete");
                    SyncMessage::send(&SyncMessage::InitialSyncAck, &mut conn).await?;

                    if let Some(notifier) = initial_sync_notifier.take() {
                        let _ = notifier.send(());
                    }
                }
                other => {
                    warn!("File sync: unexpected message: {other:?}");
                }
            }
        }
    }

    async fn handle_changeset(
        dir_state: &mut HashMap<String, DirectoryState>,
        conn: &mut VsockStream,
        directory: &str,
        file_entries: &[FileEntry],
    ) -> Result<()> {
        let dir_path = Path::new(directory);

        // Step 1: Ensure target directory exists
        fs::create_dir_all(dir_path)
            .await
            .context(format!("creating target directory {directory}"))?;

        // Step 2: Create timestamped directory
        let now = chrono::Utc::now();
        let ts_dir_name = format!("..{}", now.format("%Y_%m_%d_%H_%M_%S%.6f"));
        let ts_dir_path = dir_path.join(&ts_dir_name);

        fs::create_dir_all(&ts_dir_path).await.context(format!(
            "creating timestamped directory {}",
            ts_dir_path.display()
        ))?;

        info!(
            "File sync: receiving changeset for {directory} -> {ts_dir_name} ({} files)",
            file_entries.len()
        );

        // Step 3: Receive and write files into timestamped directory
        let write_result = Self::receive_files(conn, &ts_dir_path, file_entries).await;

        if let Err(err) = &write_result {
            // Cleanup incomplete timestamped directory on error
            error!("File sync: error receiving files for {directory}, cleaning up: {err}");
            let _ = fs::remove_dir_all(&ts_dir_path).await;
            // Still need to read ChangesetEnd to keep protocol in sync
            let _end: SyncMessage = SyncMessage::recv(conn).await?;
            return write_result;
        }

        // Step 4: Read ChangesetEnd and verify
        let end_msg: SyncMessage = SyncMessage::recv(conn).await?;
        match end_msg {
            SyncMessage::ChangesetEnd { directory: ref d } if d == directory => {}
            _ => {
                let _ = fs::remove_dir_all(&ts_dir_path).await;
                return Err(anyhow!(
                    "expected ChangesetEnd for {directory}, got {end_msg:?}"
                ));
            }
        }

        // Step 5: Create ..data_tmp symlink pointing to the timestamped directory
        let data_tmp_path = dir_path.join("..data_tmp");

        // Remove stale ..data_tmp if it exists
        let _ = fs::remove_file(&data_tmp_path).await;

        tokio::fs::symlink(&ts_dir_name, &data_tmp_path)
            .await
            .context("creating ..data_tmp symlink")?;

        // Step 6: Atomic rename ..data_tmp -> ..data
        let data_path = dir_path.join("..data");
        fs::rename(&data_tmp_path, &data_path)
            .await
            .context("atomic rename ..data_tmp to ..data")?;

        // Step 7: Create per-file symlinks (first sync only for this directory)
        let state = dir_state
            .entry(directory.to_string())
            .or_insert_with(|| DirectoryState {
                current_ts_dir: None,
                symlinks_created: false,
            });

        if !state.symlinks_created {
            for entry in file_entries {
                let symlink_path = dir_path.join(&entry.name);
                let symlink_target = PathBuf::from("..data").join(&entry.name);

                // Remove any existing file/symlink at this path
                let _ = fs::remove_file(&symlink_path).await;

                tokio::fs::symlink(&symlink_target, &symlink_path)
                    .await
                    .context(format!(
                        "creating file symlink {} -> {}",
                        symlink_path.display(),
                        symlink_target.display()
                    ))?;

                info!(
                    "File sync: created symlink {} -> {}",
                    symlink_path.display(),
                    symlink_target.display()
                );
            }
            state.symlinks_created = true;
        }

        // Step 8: Cleanup old timestamped directory
        if let Some(ref old_ts_dir) = state.current_ts_dir {
            let old_path = dir_path.join(old_ts_dir);
            if let Err(err) = fs::remove_dir_all(&old_path).await {
                warn!(
                    "File sync: failed to clean up old directory {}: {err}",
                    old_path.display(),
                );
            }
        }

        // Step 9: Update state
        state.current_ts_dir = Some(ts_dir_name.clone());

        info!("File sync: changeset for {directory} complete");

        Ok(())
    }

    async fn receive_files(
        conn: &mut VsockStream,
        ts_dir_path: &Path,
        file_entries: &[FileEntry],
    ) -> Result<()> {
        for entry in file_entries {
            let file_path = ts_dir_path.join(&entry.name);

            // Write to a temp file first, then rename for crash safety
            let tmp_path = ts_dir_path.join(format!(".tmp.{}", &entry.name));

            {
                let mut file = fs::File::create(&tmp_path)
                    .await
                    .context(format!("creating temp file for {}", entry.name))?;

                if entry.size > 0 {
                    let mut buffer = [0u8; files::SYNC_IO_BUFFER_SIZE];
                    let mut remaining = entry.size;

                    while remaining > 0 {
                        let to_read = std::cmp::min(remaining as usize, files::SYNC_IO_BUFFER_SIZE);
                        conn.read_exact(&mut buffer[..to_read])
                            .await
                            .context(format!("reading file content for {}", entry.name))?;
                        file.write_all(&buffer[..to_read]).await?;
                        remaining -= to_read as u64;
                    }
                }

                file.flush().await?;
            }

            fs::rename(&tmp_path, &file_path)
                .await
                .context(format!("renaming temp file to {}", file_path.display()))?;
        }

        Ok(())
    }
}

pub struct FileSyncService {
    server: Option<JoinHandle<()>>,
}

impl FileSyncService {
    pub async fn start(config: &Configuration) -> Result<Self> {
        let has_files = config
            .manifest
            .files
            .as_ref()
            .is_some_and(|f| !f.is_empty());

        let task = if !has_files {
            None
        } else {
            info!("Starting file sync server");

            let (sync_tx, sync_rx) = oneshot::channel();
            let server = FileSyncServer::new(sync_tx)?;

            let handle = tokio::task::spawn(async move {
                if let Err(err) = server.serve().await {
                    error!("File sync server error: {err}");
                }
            });

            info!("Waiting for initial file sync");
            let _ = sync_rx.await;
            info!("Initial file sync complete");

            Some(handle)
        };

        Ok(Self { server: task })
    }

    pub async fn stop(self) {
        if let Some(server) = self.server {
            server.abort();
            _ = server.await;
        }
    }
}
