use anyhow::{anyhow, Context, Result};
use futures::{Stream, StreamExt};
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_vsock::VsockStream;

use crate::config::Configuration;
use enclaver::constants::FILE_SYNC_PORT;
use enclaver::files;
use enclaver::json_transport::JsonTransport;
use enclaver::vsock;

struct FileSyncServer {
    files: Arc<HashMap<String, PathBuf>>,
    incoming: Box<dyn Stream<Item = VsockStream> + Unpin + Send>,
    initial_sync_done: Arc<AtomicBool>,
    initial_sync_notifier: oneshot::Sender<()>,
}

impl FileSyncServer {
    pub fn new(sender: oneshot::Sender<()>, files: &HashSet<String>) -> Result<Self> {
        let incoming = Box::new(vsock::serve(FILE_SYNC_PORT)?);

        Ok(Self {
            files: Arc::new(
                files
                    .iter()
                    .map(|file| (file.clone(), PathBuf::from(file)))
                    .collect::<HashMap<_, _>>(),
            ),
            incoming,
            initial_sync_done: Arc::new(AtomicBool::new(false)),
            initial_sync_notifier: sender,
        })
    }

    pub async fn serve(self) -> Result<()> {
        for path in self.files.values() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await.context(format!(
                    "cannot create parent directory for {}",
                    path.to_string_lossy()
                ))?;
            } else {
                return Err(anyhow!(
                    "cannot find parent directory for {}",
                    path.to_string_lossy()
                ));
            }
        }

        info!("Waiting for the host to connect for file sync");

        let mut incoming = Box::into_pin(self.incoming);
        loop {
            let initial_conn = incoming.next().await;
            if initial_conn.is_some() {
                break;
            }
        }

        info!("Accepting file sync connections");

        let (event_tx, mut event_rx) = mpsc::channel(self.files.len());
        let mut initial_sync_files = self.files.keys().cloned().collect::<HashSet<_>>();
        let initial_sync_done = self.initial_sync_done.clone();

        let server = tokio::task::spawn(async move {
            while let Some(stream) = incoming.next().await {
                let initial_sync_done = initial_sync_done.clone();
                let files = self.files.clone();
                let tx = event_tx.clone();
                tokio::task::spawn(async move {
                    match FileSyncServer::service_conn(stream, files).await {
                        Ok(file) => {
                            if !initial_sync_done.load(Ordering::SeqCst) {
                                let _ = tx.send(file).await;
                            }
                        }
                        Err(err) => error!("{err}"),
                    }
                });
            }
        });

        let initial_sync_done = self.initial_sync_done.clone();

        while !initial_sync_done.load(Ordering::SeqCst) {
            if let Some(file) = event_rx.recv().await {
                initial_sync_files.remove(&file);
                if initial_sync_files.is_empty() {
                    initial_sync_done.store(true, Ordering::SeqCst);
                    break;
                }
            }
        }

        let _ = self.initial_sync_notifier.send(());
        server.await?;

        Ok(())
    }

    async fn service_conn(
        mut vsock: VsockStream,
        files: Arc<HashMap<String, PathBuf>>,
    ) -> Result<String> {
        let file_meta = files::Metadata::recv(&mut vsock).await?;

        if let Some(file) = files.get(&file_meta.path) {
            info!(
                "Syncing file: {} ({} bytes)",
                file_meta.path, file_meta.size
            );

            let now = chrono::Utc::now();
            let ts = now.timestamp_millis().to_string();

            let mut tmp_file = file.as_os_str().to_os_string();
            tmp_file.push(std::ffi::OsString::from(".".to_string() + &ts));

            {
                let mut file = fs::File::create(&tmp_file).await?;

                if file_meta.size > 0 {
                    let mut reader = FramedRead::new(vsock, BytesCodec::new());
                    let mut bytes_written: u64 = 0;

                    while let Some(Ok(chunk)) = reader.next().await {
                        let bytes_pending = if bytes_written + chunk.len() as u64 > file_meta.size {
                            bytes_written + chunk.len() as u64 - file_meta.size
                        } else {
                            chunk.len() as u64
                        };

                        file.write_all(&chunk[..bytes_pending as usize]).await?;
                        bytes_written += bytes_pending;
                    }

                    file.flush().await?;
                }
            }

            fs::rename(&tmp_file, &file).await?;

            info!("File sync done: {}", file_meta.path);
        } else {
            return Err(anyhow!(
                "received sync request for unknown file {}",
                file_meta.path
            ));
        }

        Ok(file_meta.path)
    }
}

pub struct FileSyncService {
    server: Option<JoinHandle<()>>,
}

impl FileSyncService {
    pub async fn start(config: &Configuration) -> Result<Self> {
        let mut files = HashSet::new();

        if let Some(ref manifest_files) = config.manifest.files {
            for file_path in manifest_files {
                files.insert(file_path.clone());
            }
        }

        let task = if files.is_empty() {
            None
        } else {
            info!("Starting file sync server");

            let (sync_tx, sync_rx) = oneshot::channel();
            let server = FileSyncServer::new(sync_tx, &files)?;

            let handle = tokio::task::spawn(async move {
                if let Err(err) = server.serve().await {
                    error!("{err}");
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
