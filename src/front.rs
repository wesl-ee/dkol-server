//! First server (server list) and login server

use crate::lobby::{Ev, LoginRes};
use crate::wire::{Dec, Enc, read_frame, write_frame};
use crate::{Args, LOBBY_PORT, LOGIN_PORT};
use eyre::{Result, ensure};
use openssl::ssl::{Ssl, SslContext};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_openssl::SslStream;
use tracing::{debug, info};

const VERSION: &str = "1.10.0063";

pub async fn first(mut s: TcpStream, args: Arc<Args>) -> Result<()> {
    let mut f = Enc::new(41)
        .str(VERSION, 12)
        .ip(Some(args.addr), 20)
        .str(&LOGIN_PORT.to_string(), 8);
    for _ in 1..10 {
        f = f.ip(None, 20).str("", 8);
    }
    f = f.str("dkol", 20);
    for _ in 1..10 {
        f = f.str("", 20);
    }
    // 10 x state (0 = up), 10 x logged-in count, maintenance flag
    let f = f
        .bytes(&[0; 10 + 20 + 1])
        .str(&args.motd, args.motd.len() + 1);
    write_frame(&mut s, &f.finish()).await
}

pub async fn login(
    mut s: TcpStream,
    ctx: SslContext,
    hub: mpsc::UnboundedSender<Ev>,
    args: Arc<Args>,
) -> Result<()> {
    write_frame(&mut s, &Enc::new(37).finish()).await?;
    let f = read_frame(&mut s).await?;
    ensure!(f.id == 28, "expected VERSION_REQ, got {}", f.id);
    info!("client version {}", Dec::new(&f.body).str(12)?);
    write_frame(&mut s, &Enc::new(40).i16(0).finish()).await?;

    // in-band upgrade: the client starts its handshake right after VERSION_RES
    let mut s = SslStream::new(Ssl::new(&ctx)?, s)?;
    Pin::new(&mut s).accept().await?;

    loop {
        let f = read_frame(&mut s).await?;
        let mut d = Dec::new(&f.body);
        match f.id {
            // LOGIN_REQ. REGIST_USER_REQ never comes: we don't answer REQ_NEW_LOGIN
            5 => {
                let (_uid, cert, pwd) = (d.i32()?, d.str(20)?, d.str(20)?);
                let (reply, rx) = oneshot::channel();
                hub.send(Ev::Login {
                    cert: cert.clone(),
                    pwd,
                    reply,
                })?;
                let res = rx.await?;
                match &res {
                    Ok((uid, _, _, name)) => info!("login {cert} as {name}, uid {uid}"),
                    Err(res) => info!("login {cert}: result {res}"),
                }
                write_frame(&mut s, &login_res(res)).await?;
            }
            7 => {
                let n = args.lobbies as usize;
                let mut f = Enc::new(7).i32(n as i32);
                for i in 0..6 {
                    f = f.i32(if i < n {
                        (LOBBY_PORT + i as u16).into()
                    } else {
                        0
                    });
                }
                for i in 0..6 {
                    f = f.i16(if i < n { 400 } else { 0 });
                }
                for i in 0..6 {
                    f = f.str(
                        &if i < n {
                            format!("ロビー{}", i + 1)
                        } else {
                            String::new()
                        },
                        20,
                    );
                }
                for _ in 0..6 {
                    f = f.str("", 256);
                }
                // client closes and moves on to the lobby port
                return write_frame(&mut s, &f.finish()).await;
            }
            id => debug!("login: unhandled id {id}"),
        }
    }
}

fn login_res(res: LoginRes) -> Vec<u8> {
    let e = Enc::new(5);
    match res {
        Ok((uid, ticket, [color, job, sex], name)) => e
            .i32(uid)
            .i32(ticket)
            .i16(0)
            .str(&name, 18)
            .bytes(&[color, job, sex, 0]),
        Err(res) => e.i32(0).i32(0).i16(res).str("", 18).bytes(&[0; 4]),
    }
    .finish()
}
