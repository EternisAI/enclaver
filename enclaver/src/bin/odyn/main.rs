#![allow(clippy::new_without_default)]

pub mod api;
pub mod config;
pub mod console;
pub mod egress;
pub mod enclave;
pub mod file_sync;
pub mod ingress;
pub mod kms_proxy;
pub mod launcher;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use log::{error, info, warn};
use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::Arc;
use tokio::time;

use enclaver::constants::{APP_LOG_PORT, ENV_SYNC_PORT, STATUS_PORT};
use enclaver::nsm::Nsm;

use api::ApiService;
use config::Configuration;
use console::{AppLog, AppStatus};
use egress::EgressService;
use file_sync::FileSyncService;
use ingress::IngressService;
use kms_proxy::KmsProxyService;

const ENV_SYNC_TIMEOUT: time::Duration = time::Duration::from_secs(10);

#[derive(Parser)]
struct CliArgs {
    #[clap(long = "no-bootstrap", action)]
    no_bootstrap: bool,

    #[clap(long = "no-console", action)]
    no_console: bool,

    #[clap(long = "config-dir")]
    config_dir: String,

    #[clap(required = true)]
    entrypoint: Vec<OsString>,

    #[clap(long = "verbose", short = 'v', action = clap::ArgAction::Count)]
    verbosity: u8,
}

async fn launch(args: &CliArgs) -> Result<launcher::ExitStatus> {
    let config = Arc::new(Configuration::load(&args.config_dir).await?);

    let nsm = Arc::new(Nsm::new());

    if !args.no_bootstrap {
        enclave::bootstrap(nsm.clone()).await?;
        info!("Enclave initialized");
    }

    let egress = EgressService::start(&config).await?;
    let ingress = IngressService::start(&config)?;
    let kms_proxy = KmsProxyService::start(config.clone(), nsm.clone()).await?;
    let api = ApiService::start(&config, nsm.clone()).await?;
    let env = sync_environment(&config).await?;
    let file_sync = FileSyncService::start(&config).await?;

    let creds = launcher::Credentials { uid: 0, gid: 0 };

    info!("Starting {:?}", args.entrypoint);
    let exit_status = launcher::start_child(args.entrypoint.clone(), creds, env).await??;
    info!("Entrypoint {}", exit_status);

    file_sync.stop().await;
    api.stop().await;
    kms_proxy.stop().await;
    ingress.stop().await;
    egress.stop().await;

    Ok(exit_status)
}

async fn sync_environment(config: &Configuration) -> Result<HashMap<String, String>> {
    use futures::stream::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut env: HashMap<String, String> = HashMap::new();

    if let Some(ref keys) = config.manifest.env {
        if !keys.is_empty() {
            info!("Starting the environment sync");

            let mut incoming = enclaver::vsock::serve(ENV_SYNC_PORT)?;

            // Accept connections in a loop: if a connection turns out to be broken
            // (e.g., due to a tokio-vsock connect bug where the host's connect()
            // returns Ok before the listener was ready), retry with the next one.
            let env_buf = 'accept: loop {
                let mut sock = match time::timeout(ENV_SYNC_TIMEOUT, incoming.next()).await {
                    Ok(Some(sock)) => sock,
                    Ok(None) => {
                        return Err(anyhow!(
                            "Failed to accept environment sync vsock connection"
                        ));
                    }
                    Err(_) => {
                        return Err(anyhow!("Timed out while waiting for environment sync"));
                    }
                };

                let mut env_buf: Vec<u8> = Vec::new();
                let mut read_buf = vec![0u8; 1024];
                const ARG_MAX: usize = 128 * 1024;

                loop {
                    match time::timeout(ENV_SYNC_TIMEOUT, sock.read(&mut read_buf)).await {
                        Ok(Ok(0)) => {
                            // Ack to the host so it knows data was received
                            // and doesn't retry on a phantom connection.
                            let _ = sock.write_all(&[0x06]).await;
                            break 'accept env_buf;
                        }
                        Ok(Ok(n)) => {
                            if env_buf.len() + n > ARG_MAX {
                                return Err(anyhow!("Maximum environment size exceeded"));
                            }
                            env_buf.extend_from_slice(&read_buf[..n]);
                        }
                        Ok(Err(err)) => {
                            warn!("Error reading environment, will retry: {:#}", err);
                            continue 'accept;
                        }
                        Err(_) => return Err(anyhow!("Timed out while reading environment")),
                    }
                }
            };

            let mut synced_env: HashMap<String, String> = serde_json::from_slice(&env_buf)
                .context("Failed to parse the synced environment")?;

            for k in keys.iter() {
                if let Some(v) = synced_env.remove(k) {
                    info!("Syncing environment variable: {}", k);
                    env.insert(k.clone(), v);
                }
            }

            info!("Environment sync complete");
        } else {
            warn!("The list of environment keys in the manifest is empty, skipping the sync");
        }
    }

    Ok(env)
}

async fn run(args: &CliArgs) -> Result<()> {
    // Start the status and logs listeners ASAP so that if we fail to
    // initialize, we can communicate the status and stream the logs
    let app_status = AppStatus::new();
    let app_status_task = app_status.start_serving(STATUS_PORT);

    let mut console_task = None;
    if !args.no_console {
        let app_log = AppLog::with_stdio_redirect()?;
        console_task = Some(app_log.start_serving(APP_LOG_PORT));
    }

    match launch(args).await {
        Ok(exit_status) => app_status.exited(exit_status),
        Err(err) => app_status.fatal(err.to_string()),
    };

    app_status_task.await??;

    if let Some(task) = console_task {
        task.abort();
        _ = task.await;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    enclaver::utils::init_logging(args.verbosity);

    #[cfg(feature = "tracing")]
    console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .server_addr(([0, 0, 0, 0], 51000))
        .init();

    if let Err(err) = run(&args).await {
        error!("Error: {err:#}");
        std::process::exit(1);
    }
}
