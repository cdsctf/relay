//! Relay-wide TCP pacing. Each direction has a separate, shared payload budget.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Mutex,
    time::{Instant, sleep_until},
};

const COPY_BYTES: usize = 4096;
#[derive(Default)]
struct Counters {
    bytes: AtomicU64,
    waits: AtomicU64,
    wait_nanos: AtomicU64,
}

struct Pacer {
    rate: u64,
    capacity: u128,
    bucket: Mutex<Bucket>,
    counters: Counters,
}

struct Bucket {
    credit: u128,
    updated: Instant,
}

impl Pacer {
    fn new(rate: u64) -> Self {
        // One millisecond of credit avoids a timer per small write at high rates.
        // Low-rate paths get two copy chunks; idle time cannot create larger bursts.
        let burst_bytes = (rate / 1000).clamp((2 * COPY_BYTES) as u64, 256 * 1024);
        let capacity = burst_bytes as u128 * 1_000_000_000;
        Self {
            rate,
            capacity,
            bucket: Mutex::new(Bucket {
                credit: capacity,
                updated: Instant::now(),
            }),
            counters: Counters::default(),
        }
    }

    async fn acquire(&self, bytes: usize) {
        // Keep the FIFO mutex while waiting: only the next writer owns a timer.
        // A cancelled waiter neither reserves future bandwidth nor leaves a debt.
        let started = Instant::now();
        let mut bucket = self.bucket.lock().await;
        let cost = bytes as u128 * 1_000_000_000;
        loop {
            let now = Instant::now();
            let earned = now
                .duration_since(bucket.updated)
                .as_nanos()
                .saturating_mul(self.rate as u128);
            bucket.credit = bucket.credit.saturating_add(earned).min(self.capacity);
            bucket.updated = now;
            if bucket.credit >= cost {
                bucket.credit -= cost;
                break;
            }
            self.counters.waits.fetch_add(1, Ordering::Relaxed);
            let wait_ns = (cost - bucket.credit).div_ceil(self.rate as u128) as u64;
            sleep_until(now + Duration::from_nanos(wait_ns)).await;
        }
        self.counters
            .bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        let waited = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.counters
            .wait_nanos
            .fetch_add(waited, Ordering::Relaxed);
    }

    fn log(&self, direction: &'static str) {
        tracing::info!(
            direction,
            admitted_payload_bytes = self.counters.bytes.load(Ordering::Relaxed),
            pacing_waits = self.counters.waits.load(Ordering::Relaxed),
            pacing_wait_ns = self.counters.wait_nanos.load(Ordering::Relaxed),
            "TCP pacing stopped"
        );
    }
}

/// One instance per server, rather than one allowance per connection or stream.
#[derive(Clone)]
pub(crate) struct Traffic {
    upload: Option<Arc<Pacer>>,
    download: Option<Arc<Pacer>>,
}

impl Traffic {
    pub(crate) fn new(rate: Option<u64>) -> Self {
        Self {
            upload: rate.map(|r| Arc::new(Pacer::new(r))),
            download: rate.map(|r| Arc::new(Pacer::new(r))),
        }
    }

    pub(crate) async fn copy<A, B>(&self, a: &mut A, b: &mut B) -> io::Result<()>
    where
        A: AsyncRead + AsyncWrite + Unpin,
        B: AsyncRead + AsyncWrite + Unpin,
    {
        let (Some(upload), Some(download)) = (&self.upload, &self.download) else {
            tokio::io::copy_bidirectional(a, b).await?;
            return Ok(());
        };
        let (a_read, a_write) = tokio::io::split(a);
        let (b_read, b_write) = tokio::io::split(b);
        tokio::try_join!(
            copy(a_read, b_write, upload),
            copy(b_read, a_write, download)
        )?;
        Ok(())
    }

    pub(crate) fn log(&self) {
        if let Some(pacer) = &self.upload {
            pacer.log("platform-to-target");
        }
        if let Some(pacer) = &self.download {
            pacer.log("target-to-platform");
        }
    }
}

async fn copy<R, W>(mut read: R, mut write: W, pacer: &Pacer) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0; COPY_BYTES];
    loop {
        // At most one bounded chunk is read ahead per direction and target.
        let size = read.read(&mut buffer).await?;
        if size == 0 {
            return write.shutdown().await;
        }
        pacer.acquire(size).await;
        write.write_all(&buffer[..size]).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn budgets_are_shared_independent_and_bound_idle_credit() {
        let traffic = Traffic::new(Some(4096));
        let another = traffic.clone();
        let start = Instant::now();
        traffic.upload.as_ref().unwrap().acquire(4096).await;
        another.download.as_ref().unwrap().acquire(4096).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        another.upload.as_ref().unwrap().acquire(4096).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        another.upload.as_ref().unwrap().acquire(4096).await;
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        tokio::time::sleep(Duration::from_secs(10)).await;
        traffic.upload.as_ref().unwrap().acquire(4096).await;
        traffic.upload.as_ref().unwrap().acquire(4096).await;
        let resumed = Instant::now();
        traffic.upload.as_ref().unwrap().acquire(4096).await;
        assert_eq!(resumed.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_does_not_reserve_future_quota() {
        let pacer = Arc::new(Pacer::new(4096));
        pacer.acquire(8192).await;
        let other = pacer.clone();
        let task = tokio::spawn(async move { other.acquire(4096).await });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        let start = Instant::now();
        pacer.acquire(4096).await;
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        assert_eq!(pacer.counters.bytes.load(Ordering::Relaxed), 12288);
    }

    #[tokio::test(start_paused = true)]
    async fn high_rates_retain_scheduler_credit_for_small_writes() {
        let pacer = Pacer::new(125_000_000);
        let start = Instant::now();
        for _ in 0..30 {
            pacer.acquire(4096).await;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        for _ in 30..1000 {
            pacer.acquire(4096).await;
        }
        assert!(start.elapsed() >= Duration::from_millis(31));
        assert!(start.elapsed() < Duration::from_millis(40));
    }
}
