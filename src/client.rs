use crate::storage_capnp::storage;
use crate::{Page, PageId, Result};
use capnp_rpc::RpcSystem;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use futures::FutureExt;
use tokio::net::TcpStream;
use tokio::task::LocalSet;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// Cap'n Proto RPC client for talking to a storage node.
///
/// Note: This is a low-level storage client. Compute-side APIs live elsewhere.
pub struct StorageClient {
    client: storage::Client,
    _task: tokio::task::JoinHandle<()>,
}

impl StorageClient {
    pub async fn connect(addr: &str, local: &LocalSet) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let reader = reader.compat();
        let writer = writer.compat_write();

        let network = VatNetwork::new(reader, writer, Side::Client, Default::default());
        let mut rpc_system = RpcSystem::new(Box::new(network), None);
        let client: storage::Client = rpc_system.bootstrap(Side::Server);
        let task = local.spawn_local(rpc_system.map(|_| ()));

        Ok(Self {
            client,
            _task: task,
        })
    }

    pub async fn connect_local(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let reader = reader.compat();
        let writer = writer.compat_write();

        let network = VatNetwork::new(reader, writer, Side::Client, Default::default());
        let mut rpc_system = RpcSystem::new(Box::new(network), None);
        let client: storage::Client = rpc_system.bootstrap(Side::Server);
        let task = tokio::task::spawn_local(rpc_system.map(|_| ()));

        Ok(Self {
            client,
            _task: task,
        })
    }

    pub async fn get_durable_lsn(&self) -> Result<u64> {
        let request = self.client.get_durable_lsn_request();
        let response = request.send().promise.await?;
        Ok(response.get()?.get_durable_lsn())
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
        let mut request = self.client.append_txn_batch_request();
        {
            let p = request.get();
            let mut b = p.init_batch();
            b.set_request_id(request_id);
            b.set_start_lsn(start_lsn);
            b.set_end_lsn(end_lsn);
            let mut list = b.init_writes(writes.len() as u32);
            for (i, (page_id, page)) in writes.iter().enumerate() {
                let mut w = list.reborrow().get(i as u32);
                w.set_page_id(*page_id);
                w.set_page(page);
            }
        }
        let response = request.send().promise.await?;
        let r = response.get()?;
        Ok((r.get_commit_lsn(), r.get_durable_lsn()))
    }

    /// Fetch a raw page by id (latest only).
    /// Returns (page, page_lsn, durable_lsn).
    pub async fn get_page(&self, page_id: PageId) -> Result<Option<(Page, u64, u64)>> {
        let mut request = self.client.get_page_request();
        request.get().set_page_id(page_id);
        let response = request.send().promise.await?;
        let r = response.get()?;
        let durable = r.get_durable_lsn();
        if r.get_found() {
            Ok(Some((r.get_page()?.to_vec(), r.get_page_lsn(), durable)))
        } else {
            Ok(None)
        }
    }

    /// Bulk scan pages for warmup.
    pub async fn scan_pages(
        &self,
        start_page_id: PageId,
        limit: u32,
    ) -> Result<(Vec<(PageId, u64, Page)>, u64)> {
        let mut request = self.client.scan_pages_request();
        {
            let mut p = request.get();
            p.set_start_page_id(start_page_id);
            p.set_limit(limit);
        }
        let response = request.send().promise.await?;
        let r = response.get()?;
        let durable = r.get_durable_lsn();
        let pages = r.get_pages()?;
        let mut out = Vec::with_capacity(pages.len() as usize);
        for item in pages.iter() {
            out.push((
                item.get_page_id(),
                item.get_page_lsn(),
                item.get_page()?.to_vec(),
            ));
        }
        Ok((out, durable))
    }
}
