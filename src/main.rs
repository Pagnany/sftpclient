use russh::keys::*;
use russh::*;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use std::env;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::EnvFilter;

struct Client;

impl client::Handler for Client {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        // info!("check_server_key: {_server_public_key:?}");
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let file_appender = tracing_appender::rolling::never("./logs", "app.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_env_filter(EnvFilter::new("info"))
        .init();

    info!("--- SFTP Client gestartet ---");

    let args: Vec<String> = env::args().collect();

    // Wenn der Pfad nicht übergeben wurde.
    if args.len() != 4 {
        panic!();
    }
    let server = args[1].split(':').next().unwrap_or("localhost").to_string();
    let port = args[1]
        .split(':')
        .nth(1)
        .unwrap_or("22")
        .parse::<u16>()
        .unwrap_or(22);
    let username = args[2].clone();
    let password = args[3].clone();

    let config = russh::client::Config::default();
    let sh = Client {};
    let mut session = russh::client::connect(Arc::new(config), (server, port), sh).await?;

    if !session
        .authenticate_password(username, password)
        .await?
        .success()
    {
        panic!("authentication failed");
    }

    // open sftp session
    let channel = session.channel_open_session().await?;
    channel.request_subsystem(true, "sftp").await?;
    let sftp = SftpSession::new(channel.into_stream()).await?;

    info!("--- SFTP Client gestoppt ---");
    Ok(())
}

async fn upload_file_raw(
    local_path: &str,
    remote_path: &str,
    sftp: &SftpSession,
    delete_local_after_upload: bool,
) -> Result<(), anyhow::Error> {
    let mut local_file = File::open(local_path).await?;
    let mut buffer = Vec::new();
    local_file.read_to_end(&mut buffer).await?;

    let mut file = sftp
        .open_with_flags(
            remote_path,
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE | OpenFlags::READ,
        )
        .await?;
    file.write_all(&buffer).await?;
    file.shutdown().await?;

    if delete_local_after_upload {
        tokio::fs::remove_file(local_path).await?;
    }

    Ok(())
}

async fn upload_file(
    local_path: &str,
    remote_path: &str,
    sftp: &SftpSession,
    delete_local_after_upload: bool,
) {
    match upload_file_raw(local_path, remote_path, sftp, delete_local_after_upload).await {
        Ok(_) => {
            info!("Upload successful: {} to {}", local_path, remote_path);
        }
        Err(e) => {
            warn!(
                "Upload error: {} to {}. Error: {}",
                local_path, remote_path, e
            );
        }
    }
}

async fn upload_directory(
    local_dir: &str,
    remote_dir: &str,
    sftp: &SftpSession,
    delete_local_after_upload: bool,
) -> Result<(), anyhow::Error> {
    let mut entries = tokio::fs::read_dir(local_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let remote_path = format!("{}/{}", remote_dir, file_name);
            upload_file(
                path.to_str().unwrap(),
                &remote_path,
                sftp,
                delete_local_after_upload,
            )
            .await;
        }
    }
    Ok(())
}

async fn download_file_raw(
    remote_path: &str,
    local_path: &str,
    sftp: &SftpSession,
    delete_remote_after_download: bool,
) -> Result<(), anyhow::Error> {
    if !sftp.try_exists(remote_path).await? {
        return Err(anyhow::anyhow!(
            "Remote Datei existiert nicht: {}",
            remote_path
        ));
    }

    let mut remote_file = sftp.open_with_flags(remote_path, OpenFlags::READ).await?;
    let mut buffer = Vec::new();
    remote_file.read_to_end(&mut buffer).await?;

    let mut local_file = File::create(local_path).await?;
    local_file.write_all(&buffer).await?;

    if delete_remote_after_download {
        sftp.remove_file(remote_path).await?;
    }

    Ok(())
}

async fn download_file(
    remote_path: &str,
    local_path: &str,
    sftp: &SftpSession,
    delete_remote_after_download: bool,
) {
    match download_file_raw(remote_path, local_path, sftp, delete_remote_after_download).await {
        Ok(_) => {
            info!("Download successful: {} to {}", remote_path, local_path);
        }
        Err(e) => {
            warn!(
                "Download error: {} to {}. Error: {}",
                remote_path, local_path, e
            );
        }
    }
}

async fn download_directory(
    remote_dir: &str,
    local_dir: &str,
    sftp: &SftpSession,
    delete_remote_after_download: bool,
) -> Result<(), anyhow::Error> {
    let entries = sftp.read_dir(remote_dir).await?;
    for entry in entries {
        if entry.file_type().is_file() {
            let file_name = entry.file_name();
            let remote_path = format!("{}/{}", remote_dir, file_name);
            let local_path = format!("{}/{}", local_dir, file_name);
            download_file(
                &remote_path,
                &local_path,
                sftp,
                delete_remote_after_download,
            )
            .await;
        }
    }
    Ok(())
}
