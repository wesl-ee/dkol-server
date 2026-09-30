//! sqlite store: accounts, game library, ranking totals, saved matches. The hub keeps users in
//! memory and writes through; credentials and saves are only read from here.

use eyre::{Result, WrapErr};
use openssl::hash::MessageDigest;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS users (
    uid INTEGER PRIMARY KEY,
    cert TEXT NOT NULL UNIQUE,
    salt BLOB NOT NULL,
    hash BLOB NOT NULL,
    handle TEXT NOT NULL UNIQUE,
    color INTEGER NOT NULL DEFAULT 0,
    job INTEGER NOT NULL DEFAULT 0,
    sex INTEGER NOT NULL DEFAULT 0,
    zukan BLOB NOT NULL DEFAULT x'',
    money INTEGER NOT NULL DEFAULT 0,
    money_sa INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS saves (
    id INTEGER PRIMARY KEY,
    u0 INTEGER NOT NULL,
    u1 INTEGER NOT NULL,
    u2 INTEGER NOT NULL,
    u3 INTEGER NOT NULL,
    title TEXT NOT NULL,
    rule INTEGER NOT NULL,
    rule_data INTEGER NOT NULL,
    date TEXT NOT NULL,
    data BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS saves_owner ON saves (u0);
";

/// One connection: the hub is the only writer and serializes everything anyway
pub async fn open(opts: SqliteConnectOptions) -> Result<SqlitePool> {
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts.create_if_missing(true))
        .await?;
    sqlx::raw_sql(SCHEMA).execute(&db).await?;
    Ok(db)
}

pub fn hash(pwd: &str, salt: &[u8]) -> Vec<u8> {
    let mut h = vec![0; 32];
    openssl::pkcs5::pbkdf2_hmac(
        pwd.as_bytes(),
        salt,
        20_000,
        MessageDigest::sha256(),
        &mut h,
    )
    .expect("pbkdf2");
    h
}

/// Fresh salt + hash for `pwd`
pub fn salted(pwd: &str) -> (Vec<u8>, Vec<u8>) {
    let mut salt = vec![0; 16];
    openssl::rand::rand_bytes(&mut salt).expect("rand");
    let h = hash(pwd, &salt);
    (salt, h)
}

pub fn valid_password(pwd: &str) -> bool {
    (5..=14).contains(&pwd.len()) && pwd.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Creates an account under a random id and returns it. 14 hex digits was apparently the format
pub async fn add_user(db: &SqlitePool, handle: &str, pwd: &str) -> Result<String> {
    let mut b = [0; 7];
    openssl::rand::rand_bytes(&mut b)?;
    let cert: String = b.iter().map(|x| format!("{x:02x}")).collect();
    let (salt, hash) = salted(pwd);
    sqlx::query("INSERT INTO users (cert, salt, hash, handle) VALUES (?, ?, ?, ?)")
        .bind(&cert)
        .bind(&salt)
        .bind(&hash)
        .bind(handle)
        .execute(db)
        .await
        .wrap_err_with(|| format!("handle {handle} taken?"))?;
    Ok(cert)
}

/// false if there's no such account
pub async fn set_password(db: &SqlitePool, cert: &str, pwd: &str) -> Result<bool> {
    let (salt, hash) = salted(pwd);
    let r = sqlx::query("UPDATE users SET salt = ?, hash = ? WHERE cert = ?")
        .bind(&salt)
        .bind(&hash)
        .bind(cert)
        .execute(db)
        .await?;
    Ok(r.rows_affected() == 1)
}
