//! Read-only transport/admission diagnostic; constructs no order-capable venue.
use anyhow::{bail, Result};
#[tokio::main(worker_threads = 1)]
async fn main() -> Result<()> {
    for key in ["ARB_DATABASE_URL", "ARB_REDIS_URL"] {
        infrastructure::load_secret(key)?;
    }
    match std::env::args().nth(1).as_deref() {
        Some("postgres") => {
            let value = std::env::var("ARB_DATABASE_URL")?;
            let client =
                infrastructure::connect_database(infrastructure::database_config(&value)?).await?;
            let encrypted: bool = client
                .query_one(
                    "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
                    &[],
                )
                .await?
                .get(0);
            println!("{}", serde_json::json!({"encrypted":encrypted}));
        }
        Some("redis") => {
            let client = infrastructure::redis_client(&std::env::var("ARB_REDIS_URL")?)?;
            let timeout = std::time::Duration::from_secs(2);
            let mut connection = client.get_connection_with_timeout(timeout)?;
            connection.set_read_timeout(Some(timeout))?;
            let pong: String = redis::cmd("PING").query(&mut connection)?;
            if pong != "PONG" {
                bail!("Redis probe failed");
            }
            println!("{{\"authenticated_ping\":true}}");
        }
        Some("admission") => {
            infrastructure::ensure_new_exposure_allowed()?;
            println!("{{\"admitted\":true,\"orders\":0}}");
        }
        _ => bail!("expected postgres, redis or admission probe"),
    }
    Ok(())
}
