//! Bounded, causally ordered replay through the managed observer and paper worker.
use anyhow::{bail, Context, Result};
use event_bus::EventPublisher;
use market_data::model::MarketDataEvent;
use paper::{Ledger, PaperPlanner};
use runtime_clock::Clock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::Duration,
};
use tokio::sync::mpsc;
fn frames(path: &PathBuf) -> Result<(Vec<Value>, String)> {
    let file = fs::File::open(path).context("opening virtual replay")?;
    if file.metadata()?.len() > 64 * 1024 * 1024 {
        bail!("replay exceeds 64 MiB");
    }
    use std::io::Read;
    let mut source = Vec::new();
    file.take(64 * 1024 * 1024 + 1).read_to_end(&mut source)?;
    if source.len() > 64 * 1024 * 1024 {
        bail!("replay exceeds 64 MiB");
    }
    let hash = format!("{:x}", Sha256::digest(&source));
    let mut frames = Vec::new();
    let mut total_delay = 0_u64;
    for line in BufReader::new(source.as_slice()).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > 2 * 1024 * 1024 || frames.len() >= 100_000 {
            bail!("replay frame limit exceeded");
        }
        let frame: Value = serde_json::from_str(&line)?;
        let delay = match frame.get("delay_ms") {
            Some(v) => v.as_u64().context("delay_ms must be an unsigned integer")?,
            None => 0,
        };
        if delay > 60_000 {
            bail!("replay delay exceeds one minute");
        }
        total_delay = total_delay
            .checked_add(delay)
            .context("replay duration overflow")?;
        if total_delay > 86_400_000 {
            bail!("replay duration exceeds 24 hours");
        }
        if let Some(leg) = frame.get("wait_for_paper_leg") {
            if !leg.as_u64().is_some_and(|v| (1..=3).contains(&v)) {
                bail!("invalid paper leg barrier");
            }
        }
        if frame.get("replay_clock").is_some_and(|v| !v.is_boolean()) {
            bail!("replay_clock must be boolean");
        }
        let _: MarketDataEvent =
            serde_json::from_value(frame.clone()).context("invalid replay market event")?;
        frames.push(frame);
    }
    if frames.is_empty() {
        bail!("virtual replay must contain events");
    }
    Ok((frames, hash))
}
pub(super) fn configured_clock() -> Result<Clock> {
    let paper_replay = std::env::var("ARB_TRADING_MODE").as_deref() == Ok("paper")
        && std::env::var("ARB_OBSERVER_REPLAY_FILE").is_ok();
    let mode = std::env::var("ARB_REPLAY_CLOCK")
        .unwrap_or_else(|_| if paper_replay { "virtual" } else { "wall" }.into());
    match mode.as_str() {
        "wall" => Ok(Clock::Wall),
        "virtual" => {
            if !paper_replay
                || !super::replay_allowed(
                    std::env::var("ARB_ENV").ok().as_deref(),
                    std::env::var("ARB_LIVE_TRADING_ENABLED").ok().as_deref(),
                )
            {
                bail!(
                    "virtual replay requires an explicit development paper fixture and live=false"
                );
            }
            paper::validate_mode(
                "paper",
                std::env::var("ARB_PAPER_TRADING_ENABLED").as_deref() == Ok("true"),
                false,
            )?;
            let path = PathBuf::from(std::env::var("ARB_OBSERVER_REPLAY_FILE")?);
            let (frames, _) = frames(&path)?;
            let epoch = match std::env::var("ARB_REPLAY_EPOCH_MS") {
                Ok(value) => value.parse::<u64>().context("invalid replay epoch")?,
                Err(_) => frames[0]["event"]["timestamp"]
                    .as_u64()
                    .context("first replay event requires epoch timestamp")?,
            };
            if epoch == 0 || epoch > 253_402_300_799_000 {
                bail!("unsupported replay epoch");
            }
            Ok(Clock::virtual_at(epoch))
        }
        _ => bail!("ARB_REPLAY_CLOCK must be wall or virtual"),
    }
}
async fn quiescent(clock: &Clock) -> Result<Option<u64>> {
    tokio::time::timeout(Duration::from_secs(15), clock.quiescent())
        .await
        .context("virtual financial worker watchdog expired")?
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    path: PathBuf,
    sender: mpsc::Sender<MarketDataEvent>,
    mut acknowledged: mpsc::UnboundedReceiver<()>,
    ledger: Ledger,
    planner: PaperPlanner,
    events: EventPublisher,
    clock: Clock,
) -> Result<()> {
    let (frames, source_hash) = frames(&path)?;
    if ledger.has_financial_history().await? {
        ledger.disable().await?;
        ledger.audit("paper.replay_restart_quarantined",json!({"clock":"virtual","restored_at_ms":clock.now_ms(),"source_sha256":source_hash,"reason":"historical liquidity is never replayed over committed fills"})).await?;
        tokio::signal::ctrl_c().await?;
        return Ok(());
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if ledger.portfolio("USDT").await?.enabled {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("virtual replay awaiting explicit paper authorization timed out")??;
    let epoch = clock.now_ms();
    for (index, source) in frames.iter().enumerate() {
        if let Some(leg) = source["wait_for_paper_leg"].as_u64() {
            let mut found = false;
            for _ in 0..1024 {
                let deadline = quiescent(&clock).await?;
                if ledger.pending_leg_exists(leg as u32).await? {
                    found = true;
                    break;
                }
                if let Some(deadline) = deadline {
                    clock.advance(deadline, true)?;
                } else {
                    break;
                }
            }
            if !found {
                bail!("replay leg barrier has no supported pending intent");
            }
        }
        let target = clock
            .now_ms()
            .checked_add(source["delay_ms"].as_u64().unwrap_or(0))
            .context("clock overflow")?;
        loop {
            match quiescent(&clock).await? {
                Some(deadline) if deadline < target => clock.advance(deadline, true)?,
                _ => break,
            }
        }
        clock.begin_market();
        clock.advance(target, false)?;
        let mut frame = source.clone();
        if frame["replay_clock"] == true {
            frame["event"]["timestamp"] = json!(target);
            if let Some(symbols) = frame["event"]["symbols"].as_object_mut() {
                for symbol in symbols.values_mut() {
                    symbol["exchange_timestamp_ms"] = json!(target);
                }
            }
        }
        sender
            .send(serde_json::from_value(frame)?)
            .await
            .context("managed observer stopped")?;
        tokio::time::timeout(Duration::from_secs(15), acknowledged.recv())
            .await
            .context("market application watchdog expired")?
            .context("managed observer acknowledgement closed")?;
        let same_time_batch = frames.get(index + 1).is_some_and(|next| {
            next["delay_ms"].as_u64().unwrap_or(0) == 0 && next.get("wait_for_paper_leg").is_none()
        });
        if !same_time_batch {
            clock.end_market();
        }
    }
    let mut drained = false;
    for _ in 0..1024 {
        if let Some(deadline) = quiescent(&clock).await? {
            clock.advance(deadline, true)?;
        } else {
            drained = true;
            break;
        }
    }
    if !drained {
        bail!("replay exceeded bounded financial drain");
    }
    ledger.verify_replay_finished().await?;
    crate::paper_runtime::refresh_valuations(&ledger, &planner).await?;
    let payload = json!({"clock":"virtual","source_sha256":source_hash,"epoch_ms":epoch,"completed_at_ms":clock.now_ms(),"frames":frames.len(),"tie_policy":"apply all same-time market frames before approval or IOC fill","simulated":true});
    ledger
        .audit("paper.replay_finished", payload.clone())
        .await?;
    events.publish_critical_at(
        format!("paper-replay:{}:{}", ledger.account, source_hash),
        "paper.replay_finished",
        clock.now_ms(),
        payload,
    )?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}
