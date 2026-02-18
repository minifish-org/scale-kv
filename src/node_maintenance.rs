use super::StorageMaintenanceConfig;
use super::StorageMetricsInner;
use crate::Result;
use crate::page_store::{CheckpointConfig, PageStore};
use std::sync::Arc;
use std::time::Instant;

pub(super) async fn maybe_checkpoint_by_pressure(
    page_store: &Arc<PageStore>,
    maintenance: &StorageMaintenanceConfig,
    metrics: &Arc<StorageMetricsInner>,
) -> Result<bool> {
    let stats = page_store.buffer_stats();
    let over_pages = stats.dirty_count > maintenance.checkpoint.max_dirty_pages;
    let over_bytes = stats.dirty_bytes > maintenance.checkpoint.max_dirty_bytes;
    if over_pages || over_bytes {
        checkpoint_with_metrics(page_store, metrics).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(super) async fn checkpoint_with_metrics(
    page_store: &Arc<PageStore>,
    metrics: &Arc<StorageMetricsInner>,
) -> Result<()> {
    let started = Instant::now();
    page_store.checkpoint().await?;
    let ms = started.elapsed().as_millis() as u64;
    metrics
        .checkpoint_runs
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    metrics
        .checkpoint_total_duration_ms
        .fetch_add(ms, std::sync::atomic::Ordering::Relaxed);
    metrics
        .checkpoint_last_duration_ms
        .store(ms, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

pub(super) async fn maybe_checkpoint_with_config(
    page_store: &Arc<PageStore>,
    config: &CheckpointConfig,
    metrics: &Arc<StorageMetricsInner>,
) -> Result<bool> {
    if page_store.should_checkpoint(config).await {
        checkpoint_with_metrics(page_store, metrics).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}
