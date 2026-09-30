//! TLS for the login server. The client is Mentalis 1.0.13 (SSL3/TLS1.0) and accepts an untrusted
//! root as long as the CN is `dkol.sting.jp`. Lol

use eyre::{Result, WrapErr};
use openssl::asn1::Asn1Time;
use openssl::bn::BigNum;
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::ssl::{SslContext, SslFiletype, SslMethod, SslVersion};
use openssl::x509::extension::{ExtendedKeyUsage, SubjectAlternativeName};
use openssl::x509::{X509, X509NameBuilder};
use std::fs;
use std::path::Path;

const CN: &str = "dkol.sting.jp";

pub fn context(dir: &Path) -> Result<SslContext> {
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    if !cert.exists() {
        let (c, k) = self_signed()?;
        fs::create_dir_all(dir)?;
        fs::write(&cert, c.to_pem()?)?;
        fs::write(&key, k.private_key_to_pem_pkcs8()?)?;
    }
    let mut b = SslContext::builder(SslMethod::tls_server())?;
    b.set_min_proto_version(Some(SslVersion::TLS1))?;
    b.set_max_proto_version(Some(SslVersion::TLS1))?;
    b.set_cipher_list("AES128-SHA:AES256-SHA:@SECLEVEL=0")?;
    b.set_certificate_file(&cert, SslFiletype::PEM)
        .wrap_err_with(|| format!("{}", cert.display()))?;
    b.set_private_key_file(&key, SslFiletype::PEM)?;
    Ok(b.build())
}

fn self_signed() -> Result<(X509, PKey<Private>)> {
    let key = PKey::from_rsa(Rsa::generate(2048)?)?;
    let mut name = X509NameBuilder::new()?;
    name.append_entry_by_text("CN", CN)?;
    let name = name.build();
    let mut b = X509::builder()?;
    b.set_version(2)?;
    let serial = BigNum::from_u32(1)?.to_asn1_integer()?;
    b.set_serial_number(&serial)?;
    b.set_subject_name(&name)?;
    b.set_issuer_name(&name)?;
    b.set_pubkey(&key)?;
    let (from, to) = (
        Asn1Time::days_from_now(0)?,
        Asn1Time::days_from_now(365 * 20)?,
    );
    b.set_not_before(&from)?;
    b.set_not_after(&to)?;
    b.append_extension(ExtendedKeyUsage::new().server_auth().build()?)?;
    let san = SubjectAlternativeName::new()
        .dns(CN)
        .build(&b.x509v3_context(None, None))?;
    b.append_extension(san)?;
    b.sign(&key, MessageDigest::sha256())?;
    Ok((b.build(), key))
}
