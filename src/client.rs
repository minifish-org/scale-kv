use crate::http_protocol::{
    AppendTxnBatchRequest, AppendTxnBatchResponse, DurableLsnResponse, GetPageResponse, PageWrite,
    ScanPagesRequest, ScanPagesResponse,
};
use crate::{Page, PageId, Result};
use reqwest::StatusCode;
use tokio::task::LocalSet;

/// HTTP client for talking to a storage node.
///
/// The transport protocol is unified HTTP for native and wasm targets.
pub struct StorageClient {
    client: reqwest::Client,
    base_url: String,
}

impl StorageClient {
    pub async fn connect(addr: &str, _local: &LocalSet) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: normalize_base_url(addr),
        })
    }

    pub async fn connect_local(addr: &str) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: normalize_base_url(addr),
        })
    }

    pub async fn get_durable_lsn(&self) -> Result<u64> {
        let url = format!("{}/v1/storage/durable_lsn", self.base_url);
        let response = self.client.get(url).send().await.map_err(http_err)?;
        ensure_http_success(response.status())?;
        let body: DurableLsnResponse = response.json().await.map_err(http_err)?;
        Ok(body.durable_lsn)
    }

    /// Append a txn batch with compute-assigned LSN range.
    ///
    /// Aurora-style: `writes` are page after-images (pageId + raw page bytes).
    pub async fn append_txn_batch(
        &self,
        request_id: u64,
        start_lsn: u64,
        end_lsn: u64,
        writes: &[(PageId, Page)],
    ) -> Result<(u64, u64)> {
        let payload = AppendTxnBatchRequest {
            request_id,
            start_lsn,
            end_lsn,
            writes: writes
                .iter()
                .map(|(page_id, page)| PageWrite {
                    page_id: *page_id,
                    page: page.to_vec(),
                })
                .collect(),
        };
        let url = format!("{}/v1/storage/append_txn_batch", self.base_url);
        let response = self
            .client
            .post(url)
            .json(&payload)
            .send()
            .await
            .map_err(http_err)?;
        ensure_http_success(response.status())?;
        let body: AppendTxnBatchResponse = response.json().await.map_err(http_err)?;
        Ok((body.commit_lsn, body.durable_lsn))
    }

    /// Fetch a raw page by id (latest only).
    /// Returns (page, page_lsn, durable_lsn).
    pub async fn get_page(&self, page_id: PageId) -> Result<Option<(Page, u64, u64)>> {
        let url = format!("{}/v1/storage/page/{page_id}", self.base_url);
        let response = self.client.get(url).send().await.map_err(http_err)?;
        ensure_http_success(response.status())?;
        let body: GetPageResponse = response.json().await.map_err(http_err)?;
        if !body.found {
            return Ok(None);
        }
        Ok(Some((
            Page::copy_from_slice(&body.page),
            body.page_lsn,
            body.durable_lsn,
        )))
    }

    /// Bulk scan pages for warmup.
    pub async fn scan_pages(
        &self,
        start_page_id: PageId,
        limit: u32,
    ) -> Result<(Vec<(PageId, u64, Page)>, u64)> {
        let payload = ScanPagesRequest {
            start_page_id,
            limit,
        };
        let url = format!("{}/v1/storage/scan_pages", self.base_url);
        let response = self
            .client
            .post(url)
            .json(&payload)
            .send()
            .await
            .map_err(http_err)?;
        ensure_http_success(response.status())?;
        let body: ScanPagesResponse = response.json().await.map_err(http_err)?;
        let mut out = Vec::with_capacity(body.pages.len());
        for item in body.pages {
            out.push((item.page_id, item.page_lsn, Page::copy_from_slice(&item.page)));
        }
        Ok((out, body.durable_lsn))
    }
}

fn normalize_base_url(addr: &str) -> String {
    let trimmed = addr.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }
    format!("http://{trimmed}")
}

fn http_err(err: reqwest::Error) -> crate::Error {
    crate::Error::Io(std::io::Error::other(format!("http transport error: {err}")))
}

fn ensure_http_success(status: StatusCode) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    Err(crate::Error::Io(std::io::Error::other(format!(
        "http status error: {status}"
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PAGE_SIZE, StorageServer};
    use bytes::Bytes;
    use tempfile::tempdir;
    use tokio::task::LocalSet;

    #[tokio::test(flavor = "current_thread")]
    async fn test_connect_rejects_unreachable_address() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let client = StorageClient::connect("127.0.0.1:1", &local).await.unwrap();
                assert!(client.get_durable_lsn().await.is_err());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_client_roundtrip_append_get_and_scan() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let dir = tempdir().unwrap();
                let server = StorageServer::start_with_dir(
                    "127.0.0.1:0".parse().unwrap(),
                    dir.path().to_path_buf(),
                )
                .await
                .unwrap();
                let addr = server.addr().to_string();

                let client = StorageClient::connect(&addr, &local).await.unwrap();
                let _lsn0 = client.get_durable_lsn().await.unwrap();
                assert!(client.get_page(42).await.unwrap().is_none());

                let page = Bytes::from(vec![7u8; PAGE_SIZE]);
                let start_lsn = 1;
                let end_lsn = 2;
                let (_commit_lsn, durable_lsn) = client
                    .append_txn_batch(1, start_lsn, end_lsn, &[(42, page.clone())])
                    .await
                    .unwrap();
                assert!(durable_lsn >= end_lsn);

                let got = client.get_page(42).await.unwrap().unwrap();
                assert_eq!(got.0, page);
                assert_eq!(got.1, start_lsn);

                let (pages, durable_after_scan) = client.scan_pages(0, 10).await.unwrap();
                assert!(!pages.is_empty());
                assert!(durable_after_scan >= end_lsn);
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_connect_local_smoke() {
        let dir = tempdir().unwrap();
        let server =
            StorageServer::start_with_dir("127.0.0.1:0".parse().unwrap(), dir.path().to_path_buf())
                .await
                .unwrap();
        let addr = server.addr().to_string();

        let client = StorageClient::connect_local(&addr).await.unwrap();
        let _ = client.get_durable_lsn().await.unwrap();
    }
}
