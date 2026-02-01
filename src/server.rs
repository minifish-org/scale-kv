use crate::storage_capnp::storage;
use crate::node::{WalBatch, WalRecord};
use crate::{Result, StorageNode};
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};


pub struct StorageServer {
    data: Arc<StorageNode>,
    addr: SocketAddr,
}

struct StorageService {
    data: Arc<StorageNode>,
}

impl storage::Server for StorageService {
    fn get(
        &mut self,
        params: storage::GetParams,
        mut results: storage::GetResults,
    ) -> Promise<(), capnp::Error> {
        let key = match params.get() {
            Ok(params) => params.get_key(),
            Err(err) => return Promise::err(err),
        };

        let data = self.data.clone();
        Promise::from_future(async move {
            let value = task::spawn_blocking(move || data.get(key))
                .await
                .map_err(map_join_error)?;
            if let Some(value) = value {
                let mut res = results.get();
                res.set_found(true);
                res.set_value(&value);
            } else {
                results.get().set_found(false);
            }
            Ok(())
        })
    }

    fn put(
        &mut self,
        params: storage::PutParams,
        _results: storage::PutResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(params) => params,
            Err(err) => return Promise::err(err),
        };
        let key = params.get_key();
        let value = match params.get_value() {
            Ok(value) => value.to_vec(),
            Err(err) => return Promise::err(err),
        };

        let data = self.data.clone();
        Promise::from_future(async move {
            task::spawn_blocking(move || data.put(key, &value))
                .await
                .map_err(map_join_error)?;
            Ok(())
        })
    }

    fn delete(
        &mut self,
        params: storage::DeleteParams,
        mut results: storage::DeleteResults,
    ) -> Promise<(), capnp::Error> {
        let key = match params.get() {
            Ok(params) => params.get_key(),
            Err(err) => return Promise::err(err),
        };
        let data = self.data.clone();
        Promise::from_future(async move {
            let data_for_get = data.clone();
            let existed = task::spawn_blocking(move || data_for_get.get(key).is_some())
                .await
                .map_err(map_join_error)?;
            let data_for_delete = data.clone();
            task::spawn_blocking(move || data_for_delete.delete(key))
                .await
                .map_err(map_join_error)?;
            results.get().set_found(existed);
            Ok(())
        })
    }

    fn batch_put(
        &mut self,
        params: storage::BatchPutParams,
        _results: storage::BatchPutResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(params) => params,
            Err(err) => return Promise::err(err),
        };
        let items = match params.get_items() {
            Ok(items) => items,
            Err(err) => return Promise::err(err),
        };

        let mut batch = Vec::with_capacity(items.len() as usize);
        for item in items.iter() {
            let key = item.get_key();
            let value = match item.get_value() {
                Ok(value) => value.to_vec(),
                Err(err) => return Promise::err(err),
            };
            batch.push((key, value));
        }
        let data = self.data.clone();
        Promise::from_future(async move {
            task::spawn_blocking(move || {
                for (key, value) in batch {
                    data.put(key, &value);
                }
            })
            .await
            .map_err(map_join_error)?;
            Ok(())
        })
    }

    fn append_wal(
        &mut self,
        params: storage::AppendWalParams,
        _results: storage::AppendWalResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(params) => params,
            Err(err) => return Promise::err(err),
        };
        let batch = match params.get_batch() {
            Ok(batch) => batch,
            Err(err) => return Promise::err(err),
        };
        let start_lsn = batch.get_start_lsn();
        let end_lsn = batch.get_end_lsn();
        let records = match batch.get_records() {
            Ok(records) => records,
            Err(err) => return Promise::err(err),
        };

        let mut wal_records = Vec::with_capacity(records.len() as usize);
        for record in records.iter() {
            let key = match record.get_key() {
                Ok(key) => key.to_vec(),
                Err(err) => return Promise::err(err),
            };
            let value = match record.get_value() {
                Ok(value) => value.to_vec(),
                Err(err) => return Promise::err(err),
            };
            wal_records.push(WalRecord {
                lsn: record.get_lsn(),
                op: record.get_op(),
                page_id: record.get_page_id(),
                slot_id: record.get_slot_id(),
                key,
                value,
            });
        }

        let batch = WalBatch {
            start_lsn,
            end_lsn,
            records: wal_records,
        };

        let data = self.data.clone();
        Promise::from_future(async move {
            task::spawn_blocking(move || data.append_wal_batch(batch))
                .await
                .map_err(map_join_error)?
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            Ok(())
        })
    }

}

fn map_join_error(err: task::JoinError) -> capnp::Error {
    capnp::Error::failed(format!("storage task failed: {err}"))
}

impl StorageServer {
    pub async fn start(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::new());

        let data_clone = data.clone();
        tokio::task::spawn_local(async move {
            loop {
                let accept = listener.accept().await;
                let (stream, _) = match accept {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let data = data_clone.clone();
                tokio::task::spawn_local(async move {
                    let _ = handle_connection(stream, data).await;
                });
            }
        });

        Ok(Self { data, addr })
    }

    pub async fn start_with_dir(addr: SocketAddr, dir: PathBuf) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::open(dir)?);

        let data_clone = data.clone();
        tokio::task::spawn_local(async move {
            loop {
                let accept = listener.accept().await;
                let (stream, _) = match accept {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let data = data_clone.clone();
                tokio::task::spawn_local(async move {
                    let _ = handle_connection(stream, data).await;
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
