//! Frame codec and field primitives

use encoding_rs::EUC_JP;
use eyre::{Result, bail, ensure};
use std::net::Ipv4Addr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HEADER: usize = 8;

pub struct Frame {
    pub id: i16,
    pub body: Vec<u8>,
}

/// Largest SET_ZUKAN body we take
const TRANS_MAX: usize = 1 << 20;
/// GAME_FINISH / GAME_SAVE from a non-host: 65535 zeroed save bytes as b64 (87380) + 52
const ZERO_SAVE: usize = 87432;

pub async fn read_frame(r: &mut (impl AsyncRead + Unpin)) -> Result<Frame> {
    let mut h = [0; HEADER];
    r.read_exact(&mut h).await?;
    let id = i16::from_le_bytes([h[4], h[5]]);
    let len = u16::from_le_bytes([h[6], h[7]]) as usize;
    let len = match id {
        // The i16 length wraps, 21896 is the only way it does
        23 | 24 if len == ZERO_SAVE & 0xffff => ZERO_SAVE,
        // SET_ZUKAN: the TransHeader's i32 size is the real body length
        44 => {
            let mut s = [0; 4];
            r.read_exact(&mut s).await?;
            let size = i32::from_le_bytes(s) as usize;
            ensure!(
                (4..=TRANS_MAX).contains(&size),
                "frame id {id}: bad size {size}"
            );
            let mut body = s.to_vec();
            body.resize(size, 0);
            r.read_exact(&mut body[4..]).await?;
            return Ok(Frame { id, body });
        }
        _ => len,
    };
    ensure!(len >= HEADER, "frame id {id}: bad length {len}");
    let mut body = vec![0; len - HEADER];
    r.read_exact(&mut body).await?;
    Ok(Frame { id, body })
}

/// One write per frame
pub async fn write_frame(w: &mut (impl AsyncWrite + Unpin), f: &[u8]) -> Result<()> {
    w.write_all(f).await?;
    Ok(w.flush().await?)
}

/// Server->client frame builder.
pub struct Enc(Vec<u8>);

impl Enc {
    pub fn new(id: i16) -> Self {
        let mut v = b"SPS\0".to_vec();
        v.extend(id.to_le_bytes());
        v.extend([0, 0]);
        Enc(v)
    }

    pub fn u8(mut self, x: u8) -> Self {
        self.0.push(x);
        self
    }

    pub fn i16(mut self, x: i16) -> Self {
        self.0.extend(x.to_le_bytes());
        self
    }

    pub fn i32(mut self, x: i32) -> Self {
        self.0.extend(x.to_le_bytes());
        self
    }

    pub fn u64(mut self, x: u64) -> Self {
        self.0.extend(x.to_le_bytes());
        self
    }

    pub fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend(b);
        self
    }

    /// EUC-JP, NUL-padded or truncated to exactly `n` bytes.
    pub fn str(mut self, s: &str, n: usize) -> Self {
        let (b, _, _) = EUC_JP.encode(s);
        let b = &b[..b.len().min(n)];
        self.0.extend(b);
        self.0.resize(self.0.len() + n - b.len(), 0);
        self
    }

    /// Exactly `n` bytes, parsed whole by `IPAddress.Parse`: any padding fails on CLR 2.0. Leading
    /// `'0'` reads as "no address", so the second octet is padded to fill the field.
    pub fn ip(self, ip: Option<Ipv4Addr>, n: usize) -> Self {
        let Some(ip) = ip else { return self.str("", n) };
        let [a, b, c, d] = ip.octets();
        let (head, tail) = (format!("{a}."), format!(".{c}.{d}"));
        let w = n - head.len() - tail.len();
        self.str(&format!("{head}{b:0w$o}{tail}"), n)
    }

    pub fn finish(mut self) -> Vec<u8> {
        debug_assert!(self.0.len() <= i16::MAX as usize, "frame too long");
        let len = self.0.len() as i16;
        self.0[6..8].copy_from_slice(&len.to_le_bytes());
        self.0
    }
}

/// Client->server body reader.
pub struct Dec<'a>(&'a [u8]);

impl<'a> Dec<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Dec(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            bail!("short body: want {n}, have {}", self.0.len());
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_le_bytes(self.take(2)?.try_into()?))
    }

    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into()?))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }

    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }

    pub fn str(&mut self, n: usize) -> Result<String> {
        let b = self.take(n)?;
        let b = &b[..b.iter().position(|&c| c == 0).unwrap_or(n)];
        Ok(EUC_JP.decode_without_bom_handling(b).0.into_owned())
    }

    pub fn ip(&mut self, n: usize) -> Result<Option<Ipv4Addr>> {
        Ok(self.str(n)?.trim().parse().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_and_length() {
        let f = Enc::new(41).u8(7).i16(-2).finish();
        assert_eq!(f, [b'S', b'P', b'S', 0, 41, 0, 11, 0, 7, 0xfe, 0xff]);
    }

    #[test]
    fn ip_fills_field() {
        let f = Enc::new(0)
            .ip(Some(Ipv4Addr::new(10, 77, 0, 1)), 20)
            .finish();
        assert_eq!(&f[HEADER..], b"10.0000000000115.0.1");
        let f = Enc::new(0)
            .ip(Some(Ipv4Addr::new(210, 196, 202, 20)), 18)
            .finish();
        assert_eq!(&f[HEADER..], b"210.0000304.202.20");
    }

    #[tokio::test]
    async fn zero_save_length_unwraps() {
        // a non-host GAME_SAVE: TransHeader size is always 8, the i16 length wraps to 21896
        let mut f = b"DTW\0".to_vec();
        f.extend(24_i16.to_le_bytes());
        f.extend((ZERO_SAVE as u16).to_le_bytes());
        f.extend(8_i32.to_le_bytes());
        f.resize(ZERO_SAVE, b'A');
        f.extend(Enc::new(39).finish());
        let mut r = &f[..];
        let a = read_frame(&mut r).await.unwrap();
        assert_eq!((a.id, a.body.len()), (24, ZERO_SAVE - HEADER));
        assert_eq!(read_frame(&mut r).await.unwrap().id, 39);
    }

    #[test]
    fn str_roundtrip() {
        let f = Enc::new(0).str("ドカポン", 18).finish();
        assert_eq!(f.len(), HEADER + 18);
        assert_eq!(Dec::new(&f[HEADER..]).str(18).unwrap(), "ドカポン");
    }
}
