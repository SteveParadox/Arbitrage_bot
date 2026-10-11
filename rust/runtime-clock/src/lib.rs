//! Task-scoped financial time. Controls, leases and I/O watchdogs retain wall time.
use anyhow::{bail, Result};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;
tokio::task_local! { static CURRENT: Clock; }
#[derive(Clone, Default)]
pub enum Clock {
    #[default]
    Wall,
    Virtual(Arc<VirtualClock>),
}
#[derive(Default)]
struct State {
    now: u64,
    queued: usize,
    active: bool,
    deadline: Option<u64>,
    market_hold: bool,
}
pub struct VirtualClock {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}
pub struct WorkGuard(Clock);
impl Drop for WorkGuard {
    fn drop(&mut self) {
        if let Clock::Virtual(c) = &self.0 {
            let mut s = c.state.lock().unwrap();
            s.active = false;
            s.deadline = None;
            c.signal();
        }
    }
}
impl Clock {
    pub fn virtual_at(epoch_ms: u64) -> Self {
        Self::Virtual(Arc::new(VirtualClock {
            state: Mutex::new(State {
                now: epoch_ms,
                ..State::default()
            }),
            changed: watch::channel(0).0,
        }))
    }
    pub fn current() -> Self {
        CURRENT.try_with(Clone::clone).unwrap_or_default()
    }
    pub fn is_virtual(&self) -> bool {
        matches!(self, Self::Virtual(_))
    }
    pub fn now_ms(&self) -> u64 {
        match self {
            Self::Wall => wall_now_ms(),
            Self::Virtual(c) => c.state.lock().unwrap().now,
        }
    }
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        CURRENT.scope(self.clone(), future).await
    }
    pub fn queue(&self) {
        if let Self::Virtual(c) = self {
            c.state.lock().unwrap().queued += 1;
            c.signal();
        }
    }
    pub fn unqueue(&self) {
        if let Self::Virtual(c) = self {
            let mut s = c.state.lock().unwrap();
            s.queued = s.queued.checked_sub(1).expect("unbalanced paper queue");
            c.signal();
        }
    }
    pub async fn admit_work(&self) -> Result<WorkGuard> {
        if let Self::Virtual(c) = self {
            let mut changed = c.changed.subscribe();
            loop {
                {
                    let mut s = c.state.lock().unwrap();
                    if !s.market_hold {
                        if s.active {
                            bail!("virtual replay permits one financial worker");
                        }
                        s.queued = s
                            .queued
                            .checked_sub(1)
                            .expect("work without queue admission");
                        s.active = true;
                        c.signal();
                        break;
                    }
                }
                changed.changed().await?;
            }
        }
        Ok(WorkGuard(self.clone()))
    }
    pub fn begin_market(&self) {
        if let Self::Virtual(c) = self {
            c.state.lock().unwrap().market_hold = true;
            c.signal();
        }
    }
    pub fn end_market(&self) {
        if let Self::Virtual(c) = self {
            let mut s = c.state.lock().unwrap();
            s.market_hold = false;
            if s.deadline.is_some_and(|at| at <= s.now) {
                s.deadline = None;
            }
            c.signal();
        }
    }
    pub async fn quiescent(&self) -> Result<Option<u64>> {
        let Self::Virtual(c) = self else {
            return Ok(None);
        };
        let mut changed = c.changed.subscribe();
        loop {
            {
                let s = c.state.lock().unwrap();
                if s.deadline.is_some() || (!s.active && (s.queued == 0 || s.market_hold)) {
                    return Ok(s.deadline);
                }
            }
            changed.changed().await?;
        }
    }
    pub fn advance(&self, now: u64, wake: bool) -> Result<()> {
        let Self::Virtual(c) = self else {
            bail!("cannot advance wall clock");
        };
        let mut s = c.state.lock().unwrap();
        if now < s.now {
            bail!("virtual time regression");
        }
        if s.active && s.deadline.is_none() {
            bail!("cannot advance during financial commit");
        }
        if s.deadline.is_some_and(|at| at < now) {
            bail!("cannot skip an IOC deadline");
        }
        s.now = now;
        if wake && !s.market_hold && s.deadline.is_some_and(|at| at <= now) {
            s.deadline = None;
        }
        c.signal();
        Ok(())
    }
    pub async fn sleep(&self, duration: Duration) {
        let Self::Virtual(c) = self else {
            tokio::time::sleep(duration).await;
            return;
        };
        let mut changed = c.changed.subscribe();
        let deadline = {
            let mut s = c.state.lock().unwrap();
            assert!(
                s.active && s.deadline.is_none(),
                "IOC latency must belong to active paper work"
            );
            let deadline = s
                .now
                .checked_add(duration.as_millis().try_into().expect("latency overflow"))
                .expect("clock overflow");
            s.deadline = Some(deadline);
            c.signal();
            deadline
        };
        loop {
            {
                let s = c.state.lock().unwrap();
                if s.now >= deadline && s.deadline.is_none() && !s.market_hold {
                    return;
                }
            }
            changed.changed().await.expect("virtual clock closed");
        }
    }
}
impl VirtualClock {
    fn signal(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}
pub fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub fn now_ms() -> u64 {
    Clock::current().now_ms()
}
pub async fn sleep(duration: Duration) {
    Clock::current().sleep(duration).await;
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn logical_latency_is_independent_of_wall_and_task_scoped() {
        let clock = Clock::virtual_at(1000);
        clock.queue();
        let c = clock.clone();
        let worker = tokio::spawn(async move {
            c.scope(async {
                let _guard = c.admit_work().await.unwrap();
                assert_eq!(now_ms(), 1000);
                sleep(Duration::from_millis(50)).await;
                now_ms()
            })
            .await
        });
        assert_eq!(clock.quiescent().await.unwrap(), Some(1050));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!worker.is_finished());
        assert!(!Clock::current().is_virtual());
        clock.advance(1050, true).unwrap();
        assert_eq!(worker.await.unwrap(), 1050);
        assert_eq!(clock.quiescent().await.unwrap(), None);
        assert!(clock.advance(1049, true).is_err());
    }
    #[tokio::test]
    async fn same_time_market_batch_precedes_admission_and_fill() {
        let clock = Clock::virtual_at(1000);
        clock.begin_market();
        clock.queue();
        let c = clock.clone();
        let worker = tokio::spawn(async move {
            let _guard = c.admit_work().await.unwrap();
            c.sleep(Duration::from_millis(50)).await;
            c.now_ms()
        });
        assert_eq!(clock.quiescent().await.unwrap(), None);
        assert!(!worker.is_finished());
        clock.end_market();
        assert_eq!(clock.quiescent().await.unwrap(), Some(1050));
        clock.begin_market();
        clock.advance(1050, false).unwrap();
        clock.queue();
        clock.unqueue();
        tokio::task::yield_now().await;
        assert!(!worker.is_finished());
        clock.end_market();
        assert_eq!(worker.await.unwrap(), 1050);
    }
    #[tokio::test]
    async fn pending_candidates_do_not_block_active_timer() {
        let clock = Clock::virtual_at(1000);
        clock.queue();
        let guard = clock.admit_work().await.unwrap();
        clock.queue();
        let c = clock.clone();
        let sleeper = tokio::spawn(async move {
            c.sleep(Duration::from_millis(25)).await;
        });
        assert_eq!(clock.quiescent().await.unwrap(), Some(1025));
        assert!(clock.advance(1026, true).is_err());
        clock.advance(1025, true).unwrap();
        sleeper.await.unwrap();
        drop(guard);
        clock.unqueue();
        assert_eq!(clock.quiescent().await.unwrap(), None);
    }
}
