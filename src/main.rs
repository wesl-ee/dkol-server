mod db;
mod front;
mod lobby;
mod tls;
mod wire;

use clap::{Parser, Subcommand};
use encoding_rs::EUC_JP;
use eyre::{Result, WrapErr, ensure};
use sqlx::sqlite::SqliteConnectOptions;
use std::fs;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn};

const FIRST_PORT: u16 = 19999;
const LOGIN_PORT: u16 = 20001;
/// There are multiple lobbies, lobby n listens on LOBBY_PORT + n. The client treats ports < 10000 as trial mode.
const LOBBY_PORT: u16 = 20003;

/// Dokapon the World online server
#[derive(Parser)]
pub struct Args {
    /// address advertised to clients in the server list
    #[arg(long, default_value = "127.0.0.1")]
    addr: Ipv4Addr,
    /// address to listen on
    #[arg(long, default_value = "0.0.0.0")]
    bind: Ipv4Addr,
    /// number of lobbies, at most 6
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u8).range(1..=6))]
    lobbies: u8,
    /// directory for the generated TLS cert and key and the database
    #[arg(long, default_value = "data")]
    data: PathBuf,
    /// text shown in the client's info window on connect
    #[arg(long, default_value = "")]
    motd: String,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// create an account and print its generated id
    Adduser { handle: String, pwd: String },
    /// reset an account's password. the client has no working reset. this was
    /// probably wired up by Sting somehow on their billing or account webpage
    Passwd { cert: String, pwd: String },
}

async fn listen<F, Fut>(bind: Ipv4Addr, port: u16, serve: F) -> Result<()>
where
    F: Fn(TcpStream) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let l = TcpListener::bind((bind, port))
        .await
        .wrap_err_with(|| format!("bind {bind}:{port}"))?;
    info!("listening on {port}");
    tokio::spawn(async move {
        loop {
            let Ok((s, peer)) = l.accept().await else {
                continue;
            };
            let fut = serve(s);
            tokio::spawn(async move {
                if let Err(e) = fut.await {
                    warn!("{port} {peer}: {e:#}");
                }
            });
        }
    });
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Arc::new(Args::parse());
    fs::create_dir_all(&args.data)?;
    let path = args.data.join("dkol.db");
    let db = db::open(SqliteConnectOptions::new().filename(&path))
        .await
        .wrap_err_with(|| format!("{}", path.display()))?;
    match &args.cmd {
        Some(Cmd::Adduser { handle, pwd }) => {
            ensure!(
                db::valid_password(pwd),
                "password: 5 to 14 letters and digits"
            );
            // At most 8 chars, no spaces or half-width kana, EUC-JP
            let (euc, _, bad) = EUC_JP.encode(handle);
            ensure!(
                (1..=8).contains(&handle.chars().count())
                    && !bad
                    && euc.len() <= 18
                    && !handle
                        .chars()
                        .any(|c| c.is_whitespace() || ('\u{ff61}'..='\u{ff9f}').contains(&c)),
                "handle: 1 to 8 characters, no spaces or half-width kana"
            );
            println!("{}", db::add_user(&db, handle, pwd).await?);
            return Ok(());
        }
        Some(Cmd::Passwd { cert, pwd }) => {
            ensure!(
                db::valid_password(pwd),
                "password: 5 to 14 letters and digits"
            );
            ensure!(db::set_password(&db, cert, pwd).await?, "no account {cert}");
            return Ok(());
        }
        None => {}
    }
    let ctx = tls::context(&args.data)?;
    let (hub, rx) = mpsc::unbounded_channel();
    tokio::spawn(lobby::Hub::new(args.lobbies as usize, db).await?.run(rx));

    let a = args.clone();
    listen(args.bind, FIRST_PORT, move |s| front::first(s, a.clone())).await?;
    let (a, h) = (args.clone(), hub.clone());
    listen(args.bind, LOGIN_PORT, move |s| {
        front::login(s, ctx.clone(), h.clone(), a.clone())
    })
    .await?;
    for n in 0..args.lobbies as usize {
        let h = hub.clone();
        listen(args.bind, LOBBY_PORT + n as u16, move |s| {
            lobby::serve(s, n, h.clone())
        })
        .await?;
    }
    std::future::pending().await
}
