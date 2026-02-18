// Page-level redo only needs page image operations.
use crate::storage_capnp::storage;
use crate::{Page, Result, StorageMaintenanceConfig, StorageNode};
use capnp::capability::Promise;
use capnp_rpc::RpcSystem;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub struct StorageServer {
    data: Arc<StorageNode>,
    addr: SocketAddr,
}

struct StorageService {
    data: Arc<StorageNode>,
}

impl storage::Server for StorageService {
    fn get_durable_lsn(
        &mut self,
        _params: storage::GetDurableLsnParams,
        mut results: storage::GetDurableLsnResults,
    ) -> Promise<(), capnp::Error> {
        let data = self.data.clone();
        Promise::from_future(async move {
            results.get().set_durable_lsn(data.durable_lsn());
            Ok(())
        })
    }

    fn append_txn_batch(
        &mut self,
        params: storage::AppendTxnBatchParams,
        mut results: storage::AppendTxnBatchResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(p) => p,
            Err(err) => return Promise::err(err),
        };
        let batch = match params.get_batch() {
            Ok(b) => b,
            Err(err) => return Promise::err(err),
        };

        let request_id = batch.get_request_id();
        let start_lsn = batch.get_start_lsn();
        let end_lsn = batch.get_end_lsn();
        let writes = match batch.get_writes() {
            Ok(w) => w,
            Err(err) => return Promise::err(err),
        };

        let mut page_writes: Vec<(u64, Page)> = Vec::with_capacity(writes.len() as usize);
        for w in writes.iter() {
            let page_id = w.get_page_id();
            let page = match w.get_page() {
                Ok(p) => Page::copy_from_slice(p),
                Err(err) => return Promise::err(err),
            };
            page_writes.push((page_id, page));
        }

        let data = self.data.clone();
        Promise::from_future(async move {
            let commit_lsn = data
                .append_txn_batch_with_lsn_sync(request_id, start_lsn, end_lsn, page_writes)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            let mut res = results.get();
            res.set_commit_lsn(commit_lsn);
            res.set_durable_lsn(data.durable_lsn());
            Ok(())
        })
    }

    fn get_page(
        &mut self,
        params: storage::GetPageParams,
        mut results: storage::GetPageResults,
    ) -> Promise<(), capnp::Error> {
        let page_id = match params.get() {
            Ok(p) => p.get_page_id(),
            Err(err) => return Promise::err(err),
        };
        let data = self.data.clone();
        Promise::from_future(async move {
            let mut res = results.get();
            res.set_durable_lsn(data.durable_lsn());
            match data.get_page_latest(page_id).await {
                Some((page, page_lsn)) => {
                    res.set_found(true);
                    res.set_page(&page);
                    res.set_page_lsn(page_lsn);
                }
                None => {
                    res.set_found(false);
                }
            }
            Ok(())
        })
    }

    fn scan_pages(
        &mut self,
        params: storage::ScanPagesParams,
        mut results: storage::ScanPagesResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(p) => p,
            Err(err) => return Promise::err(err),
        };
        let start_page_id = params.get_start_page_id();
        let limit = params.get_limit();

        let data = self.data.clone();
        Promise::from_future(async move {
            let pages = data.scan_pages_latest(start_page_id, limit as usize).await;

            let mut res = results.get();
            res.set_durable_lsn(data.durable_lsn());
            let mut out = res.init_pages(pages.len() as u32);
            for (i, (page_id, page_lsn, page)) in pages.into_iter().enumerate() {
                let mut item = out.reborrow().get(i as u32);
                item.set_page_id(page_id);
                item.set_page_lsn(page_lsn);
                item.set_page(&page);
            }
            Ok(())
        })
    }
}

impl StorageServer {
    pub async fn start(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::new().await);

        let data_clone = data.clone();
        tokio::task::spawn(async move {
            loop {
                let accept = listener.accept().await;
                let (stream, _) = match accept {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let data = data_clone.clone();
                // Use spawn_blocking because RPC system is not Send
                tokio::task::spawn_blocking(move || {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async {
                        let _ = handle_connection(stream, data).await;
                    });
                });
            }
        });

        Ok(Self { data, addr })
    }

    pub async fn start_with_dir(addr: SocketAddr, dir: PathBuf) -> Result<Self> {
        Self::start_with_dir_and_maintenance(addr, dir, StorageMaintenanceConfig::default()).await
    }

    pub async fn start_with_dir_and_maintenance(
        addr: SocketAddr,
        dir: PathBuf,
        maintenance: StorageMaintenanceConfig,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::open_with_maintenance(dir, maintenance).await?);

        let data_clone = data.clone();
        tokio::task::spawn(async move {
            loop {
                let accept = listener.accept().await;
                let (stream, _) = match accept {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let data = data_clone.clone();
                // Use spawn_blocking because RPC system is not Send
                tokio::task::spawn_blocking(move || {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async {
                        let _ = handle_connection(stream, data).await;
                    });
                });
            }
        });

        Ok(Self { data, addr })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn data(&self) -> &StorageNode {
        &self.data
    }

    pub fn data_arc(&self) -> Arc<StorageNode> {
        Arc::clone(&self.data)
    }
}

pub async fn handle_connection(stream: TcpStream, data: Arc<StorageNode>) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let reader = reader.compat();
    let writer = writer.compat_write();

    let network = VatNetwork::new(reader, writer, Side::Server, Default::default());
    let client: storage::Client = capnp_rpc::new_client(StorageService { data });
    let rpc_system = RpcSystem::new(Box::new(network), Some(client.client));

    rpc_system.await.map_err(|err| err.into())
}
