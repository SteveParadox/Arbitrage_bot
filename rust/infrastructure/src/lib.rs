//! Financial-service transport and secrets. No permissive production TLS mode.
use anyhow::{bail, Context, Result};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer},
    ClientConfig, RootCertStore,
};
use serde_json::Value;
use std::{
    env,
    fs::{self, OpenOptions},
    io::Read,
    path::Path,
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub fn read_file(path: &Path, private: bool, limit: u64) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|_| anyhow::anyhow!("required configuration file unavailable"))?;
    let meta = file
        .metadata()
        .context("configuration file metadata unavailable")?;
    if !meta.is_file() || meta.len() > limit {
        bail!("configuration file must be a bounded regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if private && meta.permissions().mode() & 0o077 != 0 {
            bail!("secret file must deny group and other access");
        }
    }
    let mut value = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut value)
        .context("configuration file unreadable")?;
    if value.len() as u64 > limit {
        bail!("configuration file exceeds size limit");
    }
    Ok(value)
}
pub fn load_secret(name: &str) -> Result<()> {
    if let Ok(source) = env::var(format!("{name}_FILE")) {
        if env::var(name).is_ok_and(|v| !v.is_empty()) {
            bail!("inline and file-based secret sources conflict");
        }
        let bytes = read_file(Path::new(&source), true, 65536)?;
        let text = String::from_utf8(bytes)
            .map_err(|_| anyhow::anyhow!("secret file must contain UTF-8"))?;
        let text = text.trim_end_matches(['\r', '\n']);
        if text.is_empty() || text != text.trim() || text.contains(['\0', '\r', '\n']) {
            bail!("secret file contains invalid content");
        }
        env::set_var(name, text);
    }
    Ok(())
}
pub fn database_config(value: &str) -> Result<tokio_postgres::Config> {
    let mut uri = url::Url::parse(&value.replace("postgresql+psycopg:", "postgresql:"))
        .map_err(|_| anyhow::anyhow!("invalid PostgreSQL endpoint"))?;
    if !matches!(uri.scheme(), "postgresql" | "postgres") || uri.host_str().is_none() {
        bail!("invalid PostgreSQL endpoint");
    }
    let mode = env::var("ARB_DATABASE_SSLMODE").unwrap_or_else(|_| "disable".into());
    let mut pairs = Vec::new();
    for (key, value) in uri.query_pairs() {
        if key == "sslmode" {
            if value != mode {
                bail!("database URL contradicts transport policy");
            }
        } else if matches!(key.as_ref(), "sslrootcert" | "sslcert" | "sslkey") {
            bail!("database URL contradicts transport policy");
        } else {
            pairs.push((key.into_owned(), value.into_owned()));
        }
    }
    uri.set_query(None);
    if !pairs.is_empty() {
        uri.query_pairs_mut().extend_pairs(pairs);
    }
    uri.as_str()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid PostgreSQL configuration"))
}
pub async fn connect_database(
    mut config: tokio_postgres::Config,
) -> Result<tokio_postgres::Client> {
    let mode = env::var("ARB_DATABASE_SSLMODE").unwrap_or_else(|_| "disable".into());
    if env::var("ARB_ENV").as_deref() == Ok("production") && mode != "verify-full" {
        bail!("production PostgreSQL requires verify-full");
    }
    config.connect_timeout(Duration::from_secs(3));
    let client = match mode.as_str() {
        "verify-full" => {
            let ca =
                env::var("ARB_DATABASE_TLS_CA_CERT").context("PostgreSQL trusted CA required")?;
            let pem = read_file(Path::new(&ca), false, 1048576)?;
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(&pem) {
                roots.add(cert?)?;
            }
            if roots.is_empty() {
                bail!("PostgreSQL trusted CA invalid");
            }
            let _ = rustls::crypto::ring::default_provider().install_default();
            let tls = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let (client, connection) = config
                .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
                .await
                .context("verified database connection failed")?;
            tokio::spawn(async move {
                if connection.await.is_err() {
                    tracing::error!("database transport disconnected");
                }
            });
            client
        }
        "disable" => {
            config.ssl_mode(tokio_postgres::config::SslMode::Disable);
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .context("development database connection failed")?;
            tokio::spawn(async move {
                if connection.await.is_err() {
                    tracing::error!("database transport disconnected");
                }
            });
            client
        }
        _ => bail!("supported database policies are disable or verify-full"),
    };
    Ok(client)
}
pub fn redis_client(value: &str) -> Result<redis::Client> {
    let uri = url::Url::parse(value).map_err(|_| anyhow::anyhow!("invalid Redis endpoint"))?;
    if !matches!(uri.scheme(), "redis" | "rediss")
        || uri.host_str().is_none()
        || uri.query().is_some()
        || uri.fragment().is_some()
    {
        bail!("invalid Redis endpoint");
    }
    if env::var("ARB_ENV").as_deref() == Ok("production")
        && (uri.scheme() != "rediss" || uri.password().is_none_or(|v| v.is_empty()))
    {
        bail!("production Redis requires authenticated TLS");
    }
    if uri.scheme() == "rediss" {
        let ca = env::var("ARB_REDIS_TLS_CA_CERT").context("Redis trusted CA required")?;
        let pem = read_file(Path::new(&ca), false, 1048576)?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        redis::Client::build_with_tls(
            value,
            redis::TlsCertificates {
                client_tls: None,
                root_cert: Some(pem),
            },
        )
        .context("verified Redis configuration failed")
    } else {
        redis::Client::open(value).context("invalid Redis configuration")
    }
}
fn secrets() -> &'static Vec<String> {
    static SECRETS: OnceLock<Vec<String>> = OnceLock::new();
    SECRETS.get_or_init(|| {
        let mut values = Vec::new();
        for name in [
            "ARB_DATABASE_URL",
            "ARB_REDIS_URL",
            "ARB_CONTROL_API_TOKEN",
            "ARB_ENGINE_GRPC_TOKEN",
            "ARB_METRICS_TOKEN",
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "BYBIT_TESTNET_API_KEY",
            "BYBIT_TESTNET_API_SECRET",
            "BYBIT_LIVE_API_KEY",
            "BYBIT_LIVE_API_SECRET",
        ] {
            if let Ok(value) = env::var(name) {
                if value.len() >= 8 {
                    if let Ok(uri) = url::Url::parse(&value) {
                        if let Some(password) = uri.password() {
                            values.push(password.to_owned());
                        }
                    }
                    values.push(value);
                }
            }
        }
        values.sort_by_key(|v| std::cmp::Reverse(v.len()));
        values
    })
}
pub fn redact_text(text: &str) -> String {
    let mut text = text.to_owned();
    for value in secrets() {
        text = text.replace(value, "[REDACTED]");
    }
    text
}
pub fn redact_value(value: &mut Value) {
    match value {
        Value::String(s) => *s = redact_text(s),
        Value::Array(values) => {
            for value in values {
                redact_value(value);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                redact_value(value);
            }
        }
        _ => {}
    }
}
pub fn ensure_new_exposure_allowed() -> Result<()> {
    let marker = env::var("ARB_RESTORE_MARKER").unwrap_or_else(|_| "data/RESTORE_REQUIRED".into());
    match fs::symlink_metadata(marker) {
        Ok(_) => bail!("restored state requires explicit recovery"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => bail!("restore barrier unavailable"),
    }
    if env::var("ARB_ENV").as_deref() != Ok("production") {
        return Ok(());
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    for (name, default, field, max_age) in [
        (
            "ARB_FINANCIAL_INTEGRITY_FILE",
            "data/observability/integrity.json",
            "balanced",
            120.0,
        ),
        (
            "ARB_MONITORING_READINESS_FILE",
            "data/observability/monitoring.json",
            "ready_for_new_exposure",
            60.0,
        ),
    ] {
        let path = env::var(name).unwrap_or_else(|_| default.into());
        let evidence: Value = serde_json::from_slice(&read_file(Path::new(&path), false, 65536)?)?;
        let age = now
            - evidence["checked_at"]
                .as_f64()
                .context("invalid readiness timestamp")?;
        if evidence[field] != Value::Bool(true) || !(0.0..=max_age).contains(&age) {
            bail!("financial or monitoring evidence unhealthy or stale");
        }
    }
    Ok(())
}
