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
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
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
                            if let Ok(decoded) =
                                percent_encoding::percent_decode_str(password).decode_utf8()
                            {
                                values.push(decoded.into_owned());
                            }
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

/// Process-local resource reporting also works in separate PID namespaces.
pub fn process_sample() -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default();
    let mut sample = serde_json::json!({"checked_at":now});
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for (field, key, multiplier) in [
            ("VmRSS:", "rss_bytes", 1024),
            ("VmHWM:", "peak_rss_bytes", 1024),
            ("Threads:", "threads", 1),
        ] {
            if let Some(value) = status
                .lines()
                .find(|line| line.starts_with(field))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|s| s.parse::<u64>().ok())
            {
                sample[key] = Value::from(value * multiplier);
            }
        }
    }
    if let Ok(entries) = fs::read_dir("/proc/self/fd") {
        sample["open_fds"] = Value::from(entries.count());
    }
    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        if let Some((_, fields)) = stat.rsplit_once(") ") {
            let fields: Vec<_> = fields.split_whitespace().collect();
            if let (Some(user), Some(system)) = (
                fields.get(11).and_then(|s| s.parse::<u64>().ok()),
                fields.get(12).and_then(|s| s.parse::<u64>().ok()),
            ) {
                sample["cpu_ticks"] = Value::from(user + system);
            }
        }
    }
    if let Ok(io) = fs::read_to_string("/proc/self/io") {
        for field in ["read_bytes", "write_bytes"] {
            if let Some(value) = io
                .lines()
                .find_map(|line| line.strip_prefix(&format!("{field}: ")))
                .and_then(|v| v.parse::<u64>().ok())
            {
                sample[field] = Value::from(value);
            }
        }
    }
    sample["exchange_rest"] = rest_stats();
    sample
}
pub fn write_process_sample(path: &Path) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pending = path.with_extension("pending");
    let mut file = fs::File::create(&pending)?;
    file.write_all(&serde_json::to_vec(&process_sample())?)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    Ok(())
}
use std::sync::atomic::{AtomicU64, Ordering};
static REST: [[AtomicU64; 5]; 9] = [const { [const { AtomicU64::new(0) }; 5] }; 9];
const ENDPOINTS: [&str; 9] = [
    "create",
    "cancel",
    "realtime",
    "history",
    "executions",
    "wallet",
    "account",
    "time",
    "other",
];
pub struct RestProbe {
    index: usize,
    started: std::time::Instant,
    failed: bool,
}
impl RestProbe {
    pub fn start(path: &str) -> Self {
        let index = match path {
            "/v5/order/create" => 0,
            "/v5/order/cancel" => 1,
            "/v5/order/realtime" => 2,
            "/v5/order/history" => 3,
            "/v5/execution/list" => 4,
            "/v5/account/wallet-balance" => 5,
            "/v5/account/info" => 6,
            "/v5/market/time" => 7,
            _ => 8,
        };
        Self {
            index,
            started: std::time::Instant::now(),
            failed: true,
        }
    }
    pub fn response(&mut self, http: u16, code: i64) {
        if http == 429 || code == 10006 {
            REST[self.index][2].fetch_add(1, Ordering::Relaxed);
        }
        if http == 401 || (10003..=10005).contains(&code) {
            REST[self.index][3].fetch_add(1, Ordering::Relaxed);
        }
        self.failed = !(200..300).contains(&http) || code != 0;
    }
}
impl Drop for RestProbe {
    fn drop(&mut self) {
        REST[self.index][0].fetch_add(1, Ordering::Relaxed);
        if self.failed {
            REST[self.index][1].fetch_add(1, Ordering::Relaxed);
        }
        REST[self.index][4].fetch_add(
            self.started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}
pub fn rest_stats() -> Value {
    let mut result = serde_json::Map::new();
    for (index, endpoint) in ENDPOINTS.iter().enumerate() {
        let values: Vec<u64> = REST[index]
            .iter()
            .map(|v| v.load(Ordering::Relaxed))
            .collect();
        result.insert((*endpoint).into(),serde_json::json!({"requests":values[0],"errors":values[1],"rate_limits":values[2],"auth_errors":values[3],"latency_us":values[4]}));
    }
    Value::Object(result)
}
