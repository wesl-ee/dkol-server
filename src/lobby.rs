//! Lobby server

use crate::db;
use crate::wire::{Dec, Enc, Frame, read_frame, write_frame};
use eyre::{OptionExt, Result, bail, ensure};
use sqlx::SqlitePool;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

const ROOMS: usize = 20;
/// The client drops the lobby link after 60 s without server data.
const ALIVE: Duration = Duration::from_secs(20);
/// 5 bad passwords lock an account for 15 minutes, the client has a message for it (10003)
const FAILS: u8 = 5;
const LOCKOUT: Duration = Duration::from_secs(15 * 60);
/// Chat channels 1..=10, 0 is the whole lobby
const CHANNELS: i16 = 10;
/// Rows in a ranking page and saves offered on room creation
const PAGE: usize = 20;

type Tx = mpsc::UnboundedSender<Vec<u8>>;

/// uid, ticket, [color, job, sex], handle; or a LOGIN_RES error code
pub type LoginRes = Result<(i32, i32, [u8; 3], String), i16>;

pub enum Ev {
    /// LOGIN_REQ
    Login {
        cert: String,
        pwd: String,
        reply: oneshot::Sender<LoginRes>,
    },
    Open {
        conn: u32,
        lobby: usize,
        ip: Ipv4Addr,
        tx: Tx,
    },
    Frame {
        conn: u32,
        frame: Frame,
    },
    Close {
        conn: u32,
    },
}

pub async fn serve(s: TcpStream, lobby: usize, hub: mpsc::UnboundedSender<Ev>) -> Result<()> {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    let conn = NEXT.fetch_add(1, Ordering::Relaxed);
    let ip = match s.peer_addr()?.ip() {
        IpAddr::V4(ip) => ip,
        IpAddr::V6(ip) => ip.to_ipv4_mapped().ok_or_eyre("ipv6 peer")?,
    };
    let (r, mut w) = s.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    hub.send(Ev::Open {
        conn,
        lobby,
        ip,
        tx,
    })?;
    tokio::spawn(async move {
        while let Some(f) = rx.recv().await {
            if write_frame(&mut w, &f).await.is_err() {
                break;
            }
        }
    });
    let res = pump(r, conn, &hub).await;
    hub.send(Ev::Close { conn })?;
    res
}

async fn pump(mut r: OwnedReadHalf, conn: u32, hub: &mpsc::UnboundedSender<Ev>) -> Result<()> {
    loop {
        let frame = read_frame(&mut r).await?;
        hub.send(Ev::Frame { conn, frame })?;
    }
}

struct User {
    /// handle, at most 8 chars client-side
    name: String,
    ticket: i32,
    /// color, job, sex
    chr: [u8; 3],
    /// game library ("zukan"), base64 text as the client sent it
    zukan: Vec<u8>,
    /// ranking totals: money won with other humans, and alone against CPUs
    money: u64,
    money_sa: u64,
    fails: u8,
    locked: Option<Instant>,
}

struct Conn {
    tx: Tx,
    ip: Ipv4Addr,
    lobby: usize,
    /// 0 until LOBBYIN_REQ
    uid: i32,
    room: Option<usize>,
    /// from ROOMIN_REQ: UDP port and LAN address for P2P
    port: i16,
    local: Option<Ipv4Addr>,
    channel: i16,
    /// the seat and solo flag of the match this conn played, until its money is counted
    game: Option<(usize, bool)>,
}

#[derive(Default)]
struct Room {
    /// 0 empty, 1 being made, 2 waiting, 3 in game
    status: u8,
    rule: u8,
    rule_data: i32,
    title: String,
    lock: u8,
    pwd: String,
    /// conns by seat, host in 0
    seats: [Option<u32>; 4],
    /// save row this room continues or last saved into
    save: Option<i64>,
    /// seat uids of the save being continued; everyone else is turned away
    reserved: Option<[i32; 4]>,
    date: String,
}

impl Room {
    fn members(&self) -> impl Iterator<Item = u32> + '_ {
        self.seats.iter().flatten().copied()
    }

    fn free_seat(&self, uid: i32) -> Option<usize> {
        match self.reserved {
            Some(res) => res
                .iter()
                .position(|&u| u == uid)
                .filter(|&i| self.seats[i].is_none()),
            None => self.seats.iter().position(Option::is_none),
        }
    }
}

pub struct Hub {
    db: SqlitePool,
    users: BTreeMap<i32, User>,
    conns: BTreeMap<u32, Conn>,
    rooms: Vec<Vec<Room>>,
}

type UserRow = (i64, String, i64, i64, i64, Vec<u8>, i64, i64);
fn user((uid, name, c, j, s, zukan, m, msa): UserRow) -> (i32, User) {
    let u = User {
        name,
        ticket: 0,
        chr: [c as u8, j as u8, s as u8],
        zukan,
        money: m as u64,
        money_sa: msa as u64,
        fails: 0,
        locked: None,
    };
    (uid as i32, u)
}

type SaveRow = (i64, i64, i64, i64, i64, String, i64, i64, String);

impl Hub {
    pub async fn new(lobbies: usize, db: SqlitePool) -> Result<Self> {
        let rows: Vec<UserRow> = sqlx::query_as(
            "SELECT uid, handle, color, job, sex, zukan, money, money_sa FROM users",
        )
        .fetch_all(&db)
        .await?;
        let users: BTreeMap<_, _> = rows.into_iter().map(user).collect();
        info!("{} accounts", users.len());
        let rooms = (0..lobbies)
            .map(|_| (0..ROOMS).map(|_| Room::default()).collect())
            .collect();
        Ok(Hub {
            db,
            users,
            conns: BTreeMap::new(),
            rooms,
        })
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Ev>) {
        let mut alive = tokio::time::interval(ALIVE);
        loop {
            tokio::select! {
                ev = rx.recv() => match ev {
                    Some(Ev::Login { cert, pwd, reply }) => {
                        reply.send(self.login(cert, pwd).await).ok();
                    }
                    Some(Ev::Open { conn, lobby, ip, tx }) => self.open(conn, lobby, ip, tx),
                    Some(Ev::Frame { conn, frame }) => {
                        if let Err(e) = self.frame(conn, &frame).await {
                            warn!("conn {conn} id {}: {e:#}", frame.id);
                        }
                    }
                    Some(Ev::Close { conn }) => self.close(conn),
                    None => return,
                },
                _ = alive.tick() => {
                    for c in self.conns.values().filter(|c| c.uid != 0) {
                        c.tx.send(Enc::new(56).finish()).ok();
                    }
                }
            }
        }
    }

    fn open(&mut self, conn: u32, lobby: usize, ip: Ipv4Addr, tx: Tx) {
        tx.send(Enc::new(38).finish()).ok();
        let c = Conn {
            tx,
            ip,
            lobby,
            uid: 0,
            room: None,
            port: 0,
            local: None,
            channel: 0,
            game: None,
        };
        self.conns.insert(conn, c);
    }

    async fn login(&mut self, cert: String, pwd: String) -> LoginRes {
        let now = Instant::now();
        let fail = |e: sqlx::Error| {
            warn!("login {cert}: {e}");
            -1_i16
        };
        let found: Option<(i64, Vec<u8>, Vec<u8>)> =
            sqlx::query_as("SELECT uid, salt, hash FROM users WHERE cert = ?")
                .bind(&cert)
                .fetch_optional(&self.db)
                .await
                .map_err(fail)?;
        let (uid, salt, hash) = found.ok_or(10002_i16)?;
        let uid = uid as i32;
        if !self.users.contains_key(&uid) {
            let row: UserRow = sqlx::query_as(
                "SELECT uid, handle, color, job, sex, zukan, money, money_sa FROM users WHERE uid = ?",
            )
            .bind(uid)
            .fetch_one(&self.db)
            .await
            .map_err(fail)?;
            self.users.extend([user(row)]);
        }
        let u = self.users.get_mut(&uid).unwrap();
        if u.locked.is_some_and(|t| t > now) {
            return Err(10003);
        }
        if db::hash(&pwd, &salt) != hash {
            u.fails += 1;
            if u.fails >= FAILS {
                (u.fails, u.locked) = (0, Some(now + LOCKOUT));
            }
            return Err(-2);
        }
        u.fails = 0;
        let mut t = [0; 4];
        openssl::rand::rand_bytes(&mut t).ok();
        u.ticket = i32::from_le_bytes(t);
        Ok((uid, u.ticket, u.chr, u.name.clone()))
    }

    async fn frame(&mut self, c: u32, f: &Frame) -> Result<()> {
        debug!("conn {c} -> {}", f.id);
        let mut d = Dec::new(&f.body);
        let conn = self.conns.get(&c).ok_or_eyre("gone")?;
        ensure!(conn.uid != 0 || f.id == 8, "not logged in");
        let (l, room, uid) = (conn.lobby, conn.room, conn.uid);
        match f.id {
            // LOBBYIN_REQ
            8 => {
                let (uid, ticket) = (d.i32()?, d.i32()?);
                ensure!(
                    self.users.get(&uid).is_some_and(|u| u.ticket == ticket),
                    "bad ticket"
                );
                if let Some(old) = self
                    .conns
                    .iter()
                    .find(|(_, o)| o.uid == uid)
                    .map(|(&o, _)| o)
                {
                    self.close(old);
                }
                self.conns.get_mut(&c).unwrap().uid = uid;
                info!("{} joins lobby {l}", self.users[&uid].name);
                self.send(c, self.lobby_list(8, l, 0));
                // CHANGE_LOBBY_NT only updates rows the client already has
                self.lobby_cast(l, Some(c), self.users_page(49, l, 0));
            }
            // CHECK_ALIVE_RES, LOBBYOUT, LOGOUT
            39 | 33 | 26 => {}
            // GET_BLIST_REQ, no block list kept
            40 => self.send(c, Enc::new(59).i32(0).finish()),
            3 | 4 => self.chat(c, f)?,
            // REF_USER_REQ
            12 => {
                let uid = d.i32()?;
                let who = self
                    .conns
                    .iter()
                    .find(|(_, o)| o.uid == uid)
                    .map(|(&o, _)| o);
                let e = Enc::new(12).i16(if who.is_some() { 0 } else { -1 }).i16(0);
                self.send(c, self.user_row(e, who).finish());
            }
            // CHANGE_LOBBY_PAGE_REQ / CHANGE_ROOM_PAGE_REQ
            // pages are 1-based client-side
            31 => self.send(c, self.users_page(49, l, d.i16()?.max(1) as usize - 1)),
            32 => self.send(c, self.rooms_page(50, l)),
            // CHANGE_CHAT_CHANNEL_REQ
            34 => {
                let ch = d.i16()?;
                if !(0..=CHANNELS).contains(&ch) {
                    self.send(c, Enc::new(57).i16(-1).i16(0).finish());
                    return Ok(());
                }
                let old = std::mem::replace(&mut self.conns.get_mut(&c).unwrap().channel, ch);
                if old != ch {
                    self.channel_member(c, old, false);
                    self.channel_member(c, ch, true);
                }
                // channel 0 is the whole lobby, it has no member list
                let names: Vec<_> = self
                    .conns
                    .values()
                    .filter(|o| ch != 0 && o.uid != 0 && o.lobby == l && o.channel == ch)
                    .map(|o| &self.users[&o.uid].name)
                    .collect();
                let e = Enc::new(57).i16(0).i16(names.len() as i16);
                self.send(c, names.iter().fold(e, |e, n| e.str(n, 18)).finish());
            }
            // CHANGE_CHARACTER_REQ
            35 => {
                let chr = [d.u8()?, d.u8()?, d.u8()?];
                self.users.get_mut(&uid).unwrap().chr = chr;
                sqlx::query("UPDATE users SET color = ?, job = ?, sex = ? WHERE uid = ?")
                    .bind(chr[0])
                    .bind(chr[1])
                    .bind(chr[2])
                    .bind(uid)
                    .execute(&self.db)
                    .await?;
                self.lobby_user(c);
            }
            // SERCH_RANKING_REQ: 1 top, 2 around me, 3 / 4 the same for solo games
            38 => {
                let mode = d.i16()?;
                let key = |u: &User| if mode > 2 { u.money_sa } else { u.money };
                let mut ranked: Vec<_> = self.users.iter().filter(|(_, u)| key(u) > 0).collect();
                ranked.sort_by(|a, b| key(b.1).cmp(&key(a.1)).then(a.0.cmp(b.0)));
                let start = match mode {
                    2 | 4 => match ranked.iter().position(|&(&u, _)| u == uid) {
                        Some(i) => i.saturating_sub(PAGE / 2),
                        None => ranked.len(),
                    },
                    _ => 0,
                };
                let page = ranked.get(start..).unwrap_or_default();
                let page = &page[..page.len().min(PAGE)];
                let e = Enc::new(55).i16(start as i16 + 1).i16(page.len() as i16);
                let e = page.iter().fold(e, |e, &(&uid, u)| {
                    e.i32(uid)
                        .i32(uid)
                        .str(&u.name, 18)
                        .i16(0)
                        .u8(0)
                        .bytes(&u.chr)
                        .u64(u.money)
                        .u64(u.money_sa)
                });
                self.send(c, e.finish());
            }
            // ROOMIN_REQ, also how a room gets reserved for creation
            10 => {
                let (no, port, pwd, local) = (d.i16()?, d.i16()?, d.str(20)?, d.ip(18)?);
                ensure!(room.is_none(), "already in a room");
                let conn = self.conns.get_mut(&c).unwrap();
                (conn.port, conn.local) = (port, local);
                let r = (no as usize).wrapping_sub(1);
                let res = match self.rooms[l].get(r) {
                    None => -1,
                    Some(rm) if rm.status == 0 => 1,
                    Some(rm) if rm.status != 2 => -1,
                    Some(rm) if rm.lock != 0 && rm.pwd != pwd => -2,
                    Some(rm) => match rm.free_seat(uid) {
                        Some(_) => 0,
                        None if rm.reserved.is_some() => -1,
                        None => -4,
                    },
                };
                let e = Enc::new(10).i16(res);
                match res {
                    1 => {
                        let rm = &mut self.rooms[l][r];
                        (rm.status, rm.seats[0]) = (1, Some(c));
                        self.conns.get_mut(&c).unwrap().room = Some(r);
                        let e = self.saves(e.bytes(&[0; 18]), uid).await?;
                        self.send(c, e.finish());
                        self.lobby_room(31, l, r);
                    }
                    0 => {
                        let rm = &mut self.rooms[l][r];
                        let seat = rm.free_seat(uid).unwrap();
                        rm.seats[seat] = Some(c);
                        self.conns.get_mut(&c).unwrap().room = Some(r);
                        let host = self.conns[&self.rooms[l][r].seats[0].unwrap()].ip;
                        self.send(c, e.ip(Some(host), 18).finish());
                        self.room_cast(l, r, None, self.change(29, Some(c), l, Some(r)));
                        self.lobby_room(31, l, r);
                        self.lobby_user(c);
                    }
                    _ => self.send(c, e.bytes(&[0; 18]).finish()),
                }
            }
            // MAKE_ROOM_REQ: RoomData + pwd[20]
            9 => {
                let mine = |&r: &usize| {
                    self.rooms[l][r].status == 1 && self.rooms[l][r].seats[0] == Some(c)
                };
                let Some(r) = room.filter(mine) else {
                    self.send(c, Enc::new(9).i16(-1).finish());
                    return Ok(());
                };
                ensure!(f.body.len() >= 216 + 20, "short MAKE_ROOM");
                let (_id, _st, rule, rule_data) = (d.i16()?, d.u8()?, d.u8()?, d.i32()?);
                let (_ust, _date) = (d.i32()?, d.str(20)?);
                let (title, lock, _use, save) = (d.str(22)?, d.u8()?, d.u8()?, d.i32()?);
                let pwd = Dec::new(&f.body[216..]).str(20)?;
                // continuing a save: only its owner may host it, seats are fixed
                let cont = match save {
                    0 => None,
                    id => {
                        let row: Option<(i64, i64, i64, i64, String)> = sqlx::query_as(
                            "SELECT u0, u1, u2, u3, date FROM saves WHERE id = ? AND u0 = ?",
                        )
                        .bind(id)
                        .bind(uid)
                        .fetch_optional(&self.db)
                        .await?;
                        let Some((u0, u1, u2, u3, date)) = row else {
                            self.send(c, Enc::new(9).i16(-1).finish());
                            return Ok(());
                        };
                        Some((id as i64, [u0, u1, u2, u3].map(|u| u as i32), date))
                    }
                };
                let rm = &mut self.rooms[l][r];
                (rm.status, rm.rule, rm.rule_data, rm.title, rm.lock, rm.pwd) =
                    (2, rule, rule_data, title, lock, pwd);
                if let Some((id, seats, date)) = cont {
                    (rm.save, rm.reserved, rm.date) = (Some(id), Some(seats), date);
                }
                info!("room {} made: {} (save {:?})", r + 1, rm.title, rm.save);
                self.send(c, Enc::new(9).i16(0).finish());
                self.room_cast(l, r, None, self.change(29, Some(c), l, Some(r)));
                self.lobby_room(28, l, r);
                self.lobby_user(c);
            }
            // ROOMOUT_REQ
            11 => self.leave(c, Some(11)),
            // GAME_START_OK_REQ / GAME_START_NG_REQ: ready toggle
            29 | 30 => {
                let r = room.ok_or_eyre("not in a room")?;
                self.send(c, Enc::new(f.id + 16).i16(0).finish());
                self.room_cast(l, r, Some(c), self.change(f.id + 18, Some(c), l, Some(r)));
            }
            // GAME_START_REQ from the host
            22 => {
                let r = room.ok_or_eyre("not in a room")?;
                let rm = &self.rooms[l][r];
                ensure!(rm.seats[0] == Some(c), "not host");
                // SAVESTART hands every member the save; MatchStartRes reads it to the frame's end
                let data: Option<Vec<u8>> = match (rm.reserved, rm.save) {
                    (Some(_), Some(id)) => Some(
                        sqlx::query_scalar("SELECT data FROM saves WHERE id = ?")
                            .bind(id)
                            .fetch_one(&self.db)
                            .await?,
                    ),
                    _ => None,
                };
                let solo = rm.members().count() == 1;
                let seats = rm.seats;
                for (seat, m) in seats.iter().enumerate() {
                    if let Some(m) = m {
                        self.conns.get_mut(m).unwrap().game = Some((seat, solo));
                    }
                }
                self.rooms[l][r].status = 3;
                info!("room {} starts, solo {solo}", r + 1);
                for m in seats.into_iter().flatten() {
                    let e = Enc::new(if m == c { 22 } else { 44 });
                    let e = match &data {
                        Some(b) => e.i16(1).i16(0).bytes(b),
                        None => e.i16(0).i16(0),
                    };
                    self.send(m, e.finish());
                }
                self.lobby_room(31, l, r);
            }
            // GET_ZUKAN_REQ
            43 => {
                let z = &self.users[&uid].zukan;
                self.send(c, Enc::new(60).bytes(z).finish());
            }
            // SET_ZUKAN_REQ: TransHeader + base64
            44 => {
                let z = f.body.get(8..).ok_or_eyre("short SET_ZUKAN")?.to_vec();
                sqlx::query("UPDATE users SET zukan = ? WHERE uid = ?")
                    .bind(&z)
                    .bind(uid)
                    .execute(&self.db)
                    .await?;
                self.users.get_mut(&uid).unwrap().zukan = z;
            }
            // GAME_FINISH_REQ / GAME_SAVE_REQ: TransHeader, result, 4 x money, b64 save if result 1
            23 | 24 => {
                let _trans = (d.i32()?, d.i32()?);
                let (res, _) = (d.i16()?, d.i16()?);
                let money = [d.u64()?, d.u64()?, d.u64()?, d.u64()?];
                let host = room.filter(|&r| {
                    self.rooms[l][r].status == 3 && self.rooms[l][r].seats[0] == Some(c)
                });
                // everyone sends a save, only the host's holds data (the rest is zeros)
                if let (1, Some(r)) = (res, host) {
                    self.save(l, r, d.rest()).await?;
                }
                if f.id == 23 {
                    if let (0, Some(r)) = (res, host)
                        && let Some(id) = self.rooms[l][r].save.take()
                    {
                        sqlx::query("DELETE FROM saves WHERE id = ?")
                            .bind(id)
                            .execute(&self.db)
                            .await?;
                    }
                    if res == 0 {
                        self.credit(c, money).await?;
                    }
                    self.leave(c, Some(23));
                }
            }
            id => debug!("conn {c}: unhandled id {id} {:02x?}", f.body),
        }
        Ok(())
    }

    /// SaveInfo list for ROOMIN_RES CREATE: this user's saves as host
    async fn saves(&self, e: Enc, uid: i32) -> Result<Enc> {
        let rows: Vec<SaveRow> = sqlx::query_as(
            "SELECT id, u0, u1, u2, u3, title, rule, rule_data, date FROM saves
             WHERE u0 = ? ORDER BY id DESC LIMIT ?",
        )
        .bind(uid)
        .bind(PAGE as i64)
        .fetch_all(&self.db)
        .await?;
        let e = e.i16(rows.len() as i16).i16(0);
        Ok(rows
            .iter()
            .fold(e, |e, (id, u0, u1, u2, u3, title, rule, rd, date)| {
                let e = [u0, u1, u2, u3]
                    .into_iter()
                    .fold(e.i32(*id as i32), |e, &u| {
                        let name = self.users.get(&(u as i32)).map_or("", |u| &u.name);
                        e.i32(u as i32).str(name, 18).i16(0)
                    });
                e.str(title, 22)
                    .i16(0)
                    .str(date, 20)
                    .u8(*rule as u8)
                    .i16(0)
                    .u8(0)
                    .i32(*rd as i32)
            }))
    }

    /// Stores the host's save for room `r`, replacing the one it came from or saved before
    async fn save(&mut self, l: usize, r: usize, b64: &[u8]) -> Result<()> {
        ensure!(!b64.is_empty(), "empty save");
        let rm = &self.rooms[l][r];
        let u = rm.seats.map(|s| s.map_or(0, |s| self.conns[&s].uid));
        let id: i64 = sqlx::query_scalar(
            "INSERT OR REPLACE INTO saves (id, u0, u1, u2, u3, title, rule, rule_data, date, data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y/%m/%d %H:%M:%S', 'now', 'localtime'), ?)
             RETURNING id",
        )
        .bind(rm.save)
        .bind(u[0])
        .bind(u[1])
        .bind(u[2])
        .bind(u[3])
        .bind(&rm.title)
        .bind(rm.rule)
        .bind(rm.rule_data)
        .bind(b64)
        .fetch_one(&self.db)
        .await?;
        info!("room {} saved as {id}, {} bytes", r + 1, b64.len());
        self.rooms[l][r].save = Some(id);
        Ok(())
    }

    /// Adds `c`'s own final money (`~(m << 1)` per seat) to its ranking, once per match
    async fn credit(&mut self, c: u32, money: [u64; 4]) -> Result<()> {
        let conn = &self.conns[&c];
        let Some((seat, solo)) = conn.game else {
            return Ok(());
        };
        if money[seat] == 0 {
            return Ok(());
        }
        let m = ((!money[seat] as i64) >> 1).max(0) as u64;
        let uid = conn.uid;
        self.conns.get_mut(&c).unwrap().game = None;
        let u = self.users.get_mut(&uid).unwrap();
        let q = if solo {
            u.money_sa += m;
            "UPDATE users SET money_sa = money_sa + ? WHERE uid = ?"
        } else {
            u.money += m;
            "UPDATE users SET money = money + ? WHERE uid = ?"
        };
        info!("{} earns {m} (solo {solo})", u.name);
        sqlx::query(q)
            .bind(m as i64)
            .bind(uid)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    fn close(&mut self, c: u32) {
        let Some(conn) = self.conns.get(&c) else {
            return;
        };
        let (l, joined, ch) = (conn.lobby, conn.uid != 0, conn.channel);
        if joined {
            self.leave(c, None);
            self.channel_member(c, ch, false);
        }
        self.conns.remove(&c);
        if joined {
            self.lobby_cast(l, None, self.users_page(49, l, 0));
        }
    }

    /// CHANGE_CHANNEL_MEMBER to the rest of channel `ch`
    fn channel_member(&self, c: u32, ch: i16, joined: bool) {
        let conn = &self.conns[&c];
        if ch == 0 {
            return;
        }
        let f = Enc::new(58)
            .i16(ch)
            .u8(joined as u8)
            .str(&self.users[&conn.uid].name, 18)
            .finish();
        for (&o, oc) in &self.conns {
            if o != c && oc.uid != 0 && oc.lobby == conn.lobby && oc.channel == ch {
                self.send(o, f.clone());
            }
        }
    }

    /// Takes `c` out of its room. `reply` is the id of the lobby-list answer to `c` (11 or 23).
    /// A leaving host closes the room for everyone.
    fn leave(&mut self, c: u32, reply: Option<i16>) {
        let conn = &self.conns[&c];
        let l = conn.lobby;
        if let Some(r) = conn.room {
            info!("conn {c} leaves room {}", r + 1);
            let rm = &mut self.rooms[l][r];
            let (host, game) = (rm.seats[0] == Some(c), rm.status == 3);
            let i = rm.seats.iter().position(|&s| s == Some(c)).unwrap();
            rm.seats[i] = None;
            if rm.reserved.is_none() {
                rm.seats[i..].rotate_left(1);
            }
            self.conns.get_mut(&c).unwrap().room = None;
            if host || rm.members().next().is_none() {
                let rest = std::mem::take(&mut self.rooms[l][r]).seats;
                // When GAME_FINISH_NT is sent, clients are sent through their own GAME_FINISH. If result != 0, then the game was aborted
                let (id, res) = match (game, reply) {
                    (true, Some(23)) => (52, 0),
                    (true, _) => (52, -1),
                    _ => (51, 0),
                };
                for m in rest.into_iter().flatten() {
                    self.conns.get_mut(&m).unwrap().room = None;
                    self.send(m, self.lobby_list(id, l, res));
                }
            } else {
                self.room_cast(l, r, None, self.change(30, Some(c), l, Some(r)));
            }
            self.lobby_room(31, l, r);
        }
        if let Some(id) = reply {
            self.send(c, self.lobby_list(id, l, 0));
        }
        if self.conns.contains_key(&c) {
            self.lobby_user(c);
        }
    }

    fn chat(&self, c: u32, f: &Frame) -> Result<()> {
        ensure!(f.body.len() >= 144, "short chat");
        let conn = &self.conns[&c];
        let name = &self.users[&conn.uid].name;
        let (kind, ch) = (f.body[0], f.body[2] as i16);
        let to = Dec::new(&f.body[26..44]).str(18)?;
        let out = Enc::new(f.id)
            .bytes(&f.body[..8])
            .str(name, 18)
            .bytes(&f.body[26..])
            .finish();
        match (f.id, conn.room) {
            (4, Some(r)) => self.room_cast(conn.lobby, r, None, out),
            (4, None) => bail!("room chat outside a room"),
            _ if !to.is_empty() => {
                for (&o, oc) in &self.conns {
                    if oc.uid != 0 && self.users[&oc.uid].name == to {
                        self.send(o, out.clone());
                    }
                }
            }
            // KIND_LOBBY: channel chat
            _ if kind == 1 => {
                for (&o, oc) in &self.conns {
                    if oc.uid != 0 && oc.lobby == conn.lobby && oc.channel == ch {
                        self.send(o, out.clone());
                    }
                }
            }
            _ => self.lobby_cast(conn.lobby, None, out),
        }
        Ok(())
    }

    fn send(&self, c: u32, f: Vec<u8>) {
        if let Some(conn) = self.conns.get(&c) {
            conn.tx.send(f).ok();
        }
    }

    fn lobby_cast(&self, l: usize, skip: Option<u32>, f: Vec<u8>) {
        for (&c, conn) in &self.conns {
            if conn.lobby == l && conn.uid != 0 && Some(c) != skip {
                self.send(c, f.clone());
            }
        }
    }

    fn room_cast(&self, l: usize, r: usize, skip: Option<u32>, f: Vec<u8>) {
        for m in self.rooms[l][r].members() {
            if Some(m) != skip {
                self.send(m, f.clone());
            }
        }
    }

    /// CHANGE_LOBBY_NT for `c`'s user row
    fn lobby_user(&self, c: u32) {
        let l = self.conns[&c].lobby;
        self.lobby_cast(l, None, self.change(32, Some(c), l, None));
    }

    /// MAKE_ROOM_NT / CHANGE_ROOM_NT for room `r`
    fn lobby_room(&self, id: i16, l: usize, r: usize) {
        self.lobby_cast(l, None, self.change(id, None, l, Some(r)));
    }

    /// LobbyChangeData: user row, room row, and for 29/31 per-slot P2P addresses
    fn change(&self, id: i16, c: Option<u32>, l: usize, r: Option<usize>) -> Vec<u8> {
        let mut e = self.room_row(self.user_row(Enc::new(id), c), l, r);
        if id == 29 || id == 31 {
            let slots: Vec<_> = (0..4)
                .map(|i| {
                    r.and_then(|r| self.rooms[l][r].seats[i])
                        .map(|m| &self.conns[&m])
                })
                .collect();
            for s in &slots {
                e = e.ip(s.map(|s| s.ip), 18);
            }
            for s in &slots {
                e = e.ip(s.map(|s| s.local.unwrap_or(s.ip)), 18);
            }
            for s in &slots {
                e = e.i16(s.map_or(0, |s| s.port));
            }
        }
        e.finish()
    }

    /// LOBBYIN_RES layout, also used by ROOMOUT_RES, GAME_FINISH_RES, ROOMOUT_OWNER, GAME_FINISH_NT
    fn lobby_list(&self, id: i16, l: usize, res: i16) -> Vec<u8> {
        let mut e = Enc::new(id).i16(res).i16(0);
        e = self.users(e, l, 0);
        for r in 0..ROOMS {
            e = self.room_row(e, l, Some(r));
        }
        e.finish()
    }

    fn users_page(&self, id: i16, l: usize, page: usize) -> Vec<u8> {
        self.users(Enc::new(id), l, page).finish()
    }

    fn rooms_page(&self, id: i16, l: usize) -> Vec<u8> {
        (0..ROOMS)
            .fold(Enc::new(id), |e, r| self.room_row(e, l, Some(r)))
            .finish()
    }

    fn users(&self, e: Enc, l: usize, page: usize) -> Enc {
        let mut here = self
            .conns
            .iter()
            .filter(|(_, c)| c.lobby == l && c.uid != 0)
            .skip(page * 20);
        (0..20).fold(e, |e, _| self.user_row(e, here.next().map(|(&c, _)| c)))
    }

    /// UserData, 48 bytes
    fn user_row(&self, e: Enc, c: Option<u32>) -> Enc {
        let Some(conn) = c.and_then(|c| self.conns.get(&c)).filter(|c| c.uid != 0) else {
            return e.bytes(&[0; 48]);
        };
        let u = &self.users[&conn.uid];
        let room = conn.room.map(|r| &self.rooms[conn.lobby][r]);
        let status = match room {
            Some(rm) if rm.status == 3 => 3,
            Some(_) => 2,
            None => 1,
        };
        e.i32(conn.uid)
            .i32(conn.uid)
            .str(&u.name, 18)
            .i16(conn.room.map_or(0, |r| r as i16 + 1))
            .u8(status)
            .bytes(&u.chr)
            .u64(u.money)
            .u64(u.money_sa)
    }

    /// RoomData, 216 bytes
    fn room_row(&self, e: Enc, l: usize, r: Option<usize>) -> Enc {
        let Some(r) = r else {
            return e.bytes(&[0; 216]);
        };
        let rm = &self.rooms[l][r];
        // when a room continues from a save we list every saved player, the client checks joiners against it
        let users: Vec<_> = (0..4)
            .map(|i| {
                let uid = match rm.reserved {
                    Some(res) => res[i],
                    None => rm.seats[i].map_or(0, |m| self.conns[&m].uid),
                };
                Some((uid, self.users.get(&uid)?))
            })
            .collect();
        let mut e = e
            .i16(r as i16 + 1)
            .u8(rm.status)
            .u8(rm.rule)
            .i32(rm.rule_data);
        for s in &rm.seats {
            e = e.u8(if s.is_some() { 2 } else { 0 });
        }
        e = e
            .str(&rm.date, 20)
            .str(&rm.title, 22)
            .u8(rm.lock)
            .u8(rm.members().count() as u8)
            .i32(rm.reserved.and(rm.save).unwrap_or(0) as i32)
            .i32(users[0].map_or(0, |u| u.0));
        for u in &users {
            e = e.i32(u.map_or(0, |u| u.0));
        }
        for u in &users {
            e = e.str(u.map_or("", |u| &u.1.name), 18);
        }
        for u in &users {
            e = e.u64(u.map_or(0, |u| u.1.money));
        }
        for u in &users {
            e = e.u64(u.map_or(0, |u| u.1.money_sa));
        }
        e
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::HEADER;
    use sqlx::sqlite::SqliteConnectOptions;
    use std::str::FromStr;

    type Rx = mpsc::UnboundedReceiver<Vec<u8>>;

    async fn hub() -> Hub {
        let db = db::open(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
            .await
            .unwrap();
        Hub::new(1, db).await.unwrap()
    }

    async fn send(hub: &mut Hub, c: u32, id: i16, body: Vec<u8>) {
        hub.frame(c, &Frame { id, body }).await.unwrap();
    }

    /// (id, first i16 of the body) of every frame queued for a conn
    fn recv(rx: &mut Rx) -> Vec<(i16, i16)> {
        frames(rx)
            .iter()
            .map(|f| {
                let id = i16::from_le_bytes([f[4], f[5]]);
                let res = f
                    .get(HEADER..HEADER + 2)
                    .map_or(0, |b| i16::from_le_bytes([b[0], b[1]]));
                (id, res)
            })
            .collect()
    }

    fn frames(rx: &mut Rx) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn body(f: Enc) -> Vec<u8> {
        f.finish()[HEADER..].to_vec()
    }

    async fn join(hub: &mut Hub, c: u32, handle: &str) -> Rx {
        let (tx, rx) = mpsc::unbounded_channel();
        hub.open(c, 0, Ipv4Addr::new(10, 77, 0, 10 + c as u8), tx);
        let cert = db::add_user(&hub.db, handle, "passw").await.unwrap();
        let (uid, ticket, ..) = hub.login(cert, "passw".into()).await.unwrap();
        send(hub, c, 8, body(Enc::new(0).i32(uid).i32(ticket))).await;
        rx
    }

    fn room_in(no: i16) -> Vec<u8> {
        body(Enc::new(0).i16(no).i16(13801).str("", 20).ip(None, 18))
    }

    /// RoomData with title and save id + pwd[20]
    fn make_room(title: &str, save: i32) -> Vec<u8> {
        let mut b = body(
            Enc::new(0)
                .bytes(&[0; 32])
                .str(title, 22)
                .bytes(&[0; 2])
                .i32(save)
                .bytes(&[0; 156]),
        );
        b.extend([0; 20]);
        b
    }

    /// GameFinData: TransHeader, result, money per seat, save
    fn fin(res: i16, money: [i64; 4], save: &[u8]) -> Vec<u8> {
        let e = money
            .iter()
            .fold(Enc::new(0).i32(8).i32(0).i16(res).i16(0), |e, &m| {
                e.u64(if m == 0 { 0 } else { !((m << 1) as u64) })
            });
        body(e.bytes(save))
    }

    /// a hosts room 1, b joins and readies, a starts
    async fn start(hub: &mut Hub, a: &mut Rx, b: &mut Rx, save: i32) {
        send(hub, 1, 10, room_in(1)).await;
        assert_eq!(recv(a)[0], (10, 1));
        send(hub, 1, 9, make_room("test room", save)).await;
        assert_eq!(recv(a)[..2], [(9, 0), (29, 1)]);
        recv(b);
        send(hub, 2, 10, room_in(1)).await;
        assert_eq!(recv(b)[..2], [(10, 0), (29, 2)]);
        assert_eq!(recv(a)[0], (29, 2));
        send(hub, 1, 22, vec![]).await;
    }

    #[tokio::test]
    async fn login_rules() {
        let mut hub = hub().await;
        let pwd = "passw".to_string();
        assert_eq!(
            hub.login("nobody00000000".into(), pwd.clone())
                .await
                .unwrap_err(),
            10002
        );
        let cert = db::add_user(&hub.db, "a", &pwd).await.unwrap();
        assert!(cert.len() == 14 && cert.bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(db::add_user(&hub.db, "a", "other").await.is_err());
        // made after the hub loaded: picked up on first login
        assert!(hub.login(cert.clone(), pwd.clone()).await.is_ok());
        assert!(db::set_password(&hub.db, &cert, "newpw").await.unwrap());
        assert!(hub.login(cert.clone(), pwd.clone()).await.is_err());
        assert!(hub.login(cert.clone(), "newpw".into()).await.is_ok());
        for _ in 0..FAILS {
            assert_eq!(
                hub.login(cert.clone(), "wrong".into()).await.unwrap_err(),
                -2
            );
        }
        assert_eq!(
            hub.login(cert.clone(), "newpw".into()).await.unwrap_err(),
            10003
        );

        // accounts survive a restart
        let hub = Hub::new(1, hub.db.clone()).await.unwrap();
        assert_eq!(hub.users.values().next().unwrap().name, "a");
    }

    #[tokio::test]
    async fn match_flow() {
        let mut hub = hub().await;
        let mut a = join(&mut hub, 1, "a").await;
        let mut b = join(&mut hub, 2, "b").await;
        assert_eq!(recv(&mut a), [(38, 0), (8, 0), (49, 1)]);
        assert_eq!(recv(&mut b), [(38, 0), (8, 0)]);
        // page 1 is the first page
        send(&mut hub, 2, 31, body(Enc::new(0).i16(1).i16(0))).await;
        assert_eq!(recv(&mut b), [(49, 1)]);

        start(&mut hub, &mut a, &mut b, 0).await;
        assert_eq!(recv(&mut a)[0], (22, 0));
        assert_eq!(recv(&mut b)[0], (44, 0));

        // host finishes: b is told to finish too, room is freed
        send(&mut hub, 1, 23, fin(0, [500, 300, 0, 0], &[])).await;
        assert_eq!(recv(&mut b)[0], (52, 0));
        assert!(recv(&mut a).contains(&(23, 0)));
        assert_eq!(hub.rooms[0][0].status, 0);
        assert!(hub.conns.values().all(|c| c.room.is_none()));

        // each player is credited their own seat once
        send(&mut hub, 2, 23, fin(0, [500, 300, 0, 0], &[])).await;
        send(&mut hub, 2, 23, fin(0, [500, 300, 0, 0], &[])).await;
        let money = |h: &Hub, u| (h.users[&u].money, h.users[&u].money_sa);
        assert_eq!((money(&hub, 1), money(&hub, 2)), ((500, 0), (300, 0)));
        recv(&mut a);
        send(&mut hub, 1, 38, body(Enc::new(0).i16(1).i16(0))).await;
        let f = &frames(&mut a)[0];
        assert_eq!(&f[HEADER..HEADER + 4], [1, 0, 2, 0]);
        assert_eq!(Dec::new(&f[HEADER + 12..]).str(18).unwrap(), "a");
    }

    #[tokio::test]
    async fn save_and_resume() {
        let mut hub = hub().await;
        let mut a = join(&mut hub, 1, "a").await;
        let mut b = join(&mut hub, 2, "b").await;
        recv(&mut a);
        recv(&mut b);
        start(&mut hub, &mut a, &mut b, 0).await;

        // the non-host's save is zeros and ignored, the host's is kept; then save and quit
        send(&mut hub, 2, 24, fin(1, [0; 4], b"AAAA")).await;
        send(&mut hub, 1, 24, fin(1, [0; 4], b"c2F2ZQ==")).await;
        send(&mut hub, 1, 23, fin(1, [0; 4], b"c2F2ZTI=")).await;
        send(&mut hub, 2, 23, fin(1, [0; 4], b"AAAA")).await;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM saves")
            .fetch_one(&hub.db)
            .await
            .unwrap();
        assert_eq!(n, 1);
        recv(&mut a);
        recv(&mut b);

        // b can't host a's save, a sees it on create
        send(&mut hub, 2, 10, room_in(2)).await;
        let f = &frames(&mut b)[0];
        assert_eq!(&f[HEADER..HEADER + 2], [1, 0]);
        assert_eq!(&f[HEADER + 20..HEADER + 22], [0, 0]);
        send(&mut hub, 2, 11, vec![]).await;
        recv(&mut a);
        send(&mut hub, 1, 10, room_in(1)).await;
        let f = &frames(&mut a)[0];
        assert_eq!(&f[HEADER + 20..HEADER + 22], [1, 0]);
        let id = i32::from_le_bytes(f[HEADER + 24..HEADER + 28].try_into().unwrap());
        send(&mut hub, 1, 9, make_room("test room", id)).await;
        assert_eq!(hub.rooms[0][0].reserved, Some([1, 2, 0, 0]));

        // c isn't in the save
        let mut c = join(&mut hub, 3, "c").await;
        recv(&mut c);
        send(&mut hub, 3, 10, room_in(1)).await;
        assert_eq!(recv(&mut c)[0], (10, -1));

        recv(&mut b);
        send(&mut hub, 2, 10, room_in(1)).await;
        assert_eq!(recv(&mut b)[0], (10, 0));
        recv(&mut a);
        send(&mut hub, 1, 22, vec![]).await;
        let f = &frames(&mut b)[0];
        assert_eq!(&f[HEADER..HEADER + 2], [1, 0]);
        assert_eq!(&f[HEADER + 4..], b"c2F2ZTI=");

        // a finished match drops its save
        send(&mut hub, 1, 23, fin(0, [1, 1, 0, 0], &[])).await;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM saves")
            .fetch_one(&hub.db)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn channels() {
        let mut hub = hub().await;
        let mut a = join(&mut hub, 1, "a").await;
        let mut b = join(&mut hub, 2, "b").await;
        recv(&mut a);
        recv(&mut b);
        send(&mut hub, 1, 34, body(Enc::new(0).i16(3).i16(0))).await;
        assert_eq!(recv(&mut a), [(57, 0)]);
        send(&mut hub, 2, 34, body(Enc::new(0).i16(3).i16(0))).await;
        assert_eq!(recv(&mut a), [(58, 3)]);
        let f = &frames(&mut b)[0];
        assert_eq!(&f[HEADER..HEADER + 4], [0, 0, 2, 0]);

        // channel chat reaches the channel only
        let mut c = join(&mut hub, 3, "c").await;
        recv(&mut a);
        recv(&mut b);
        recv(&mut c);
        let chat = body(Enc::new(0).u8(1).u8(1).u8(3).u8(0).bytes(&[0; 140]));
        send(&mut hub, 1, 3, chat).await;
        assert_eq!(recv(&mut b), [(3, 0x0101)]);
        assert!(recv(&mut c).is_empty());
    }
}
