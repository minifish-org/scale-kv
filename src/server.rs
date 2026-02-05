use crate::storage_capnp::storage;
use crate::node::{WalBatch, WalRecord, WAL_OP_TXN_COMMIT, WAL_OP_TXN_DEL, WAL_OP_TXN_PUT};
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
            let value = data.get(key).await;
            let mut res = results.get();
            res.set_durable_lsn(data.durable_lsn());
            if let Some(value) = value {
                res.set_found(true);
                res.set_value(&value);
            } else {
                res.set_found(false);
            }
            Ok(())
        })
    }

    fn put(
        &mut self,
        params: storage::PutParams,
        mut results: storage::PutResults,
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
            let data2 = data.clone();
            task::spawn_blocking(move || data2.put(key, &value))
                .await
                .map_err(map_join_error)?;
            results.get().set_durable_lsn(data.durable_lsn());
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
            let existed = data.contains(key).await;
            data.delete(key);
            let mut res = results.get();
            res.set_found(existed);
            res.set_durable_lsn(data.durable_lsn());
            Ok(())
        })
    }

    fn batch_put(
        &mut self,
        params: storage::BatchPutParams,
        mut results: storage::BatchPutResults,
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
            let data2 = data.clone();
            task::spawn_blocking(move || {
                for (key, value) in batch {
                    data2.put(key, &value);
                }
            })
            .await
            .map_err(map_join_error)?;
            results.get().set_durable_lsn(data.durable_lsn());
            Ok(())
        })
    }

    fn append_wal(
        &mut self,
        params: storage::AppendWalParams,
        mut results: storage::AppendWalResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(params) => params,
            Err(err) => return Promise::err(err),
        };
        let batch = match params.get_batch() {
            Ok(batch) => batch,
            Err(err) => return Promise::err(err),
        };
        let request_id = batch.get_request_id();
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
            request_id,
            start_lsn,
            end_lsn,
            records: wal_records,
        };

        let data = self.data.clone();
        Promise::from_future(async move {
            let durable = data
                .append_wal_batch_sync(batch)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            results.get().set_durable_lsn(durable);
            Ok(())
        })
    }

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

    fn txn_get(
        &mut self,
        params: storage::TxnGetParams,
        mut results: storage::TxnGetResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(p) => p,
            Err(err) => return Promise::err(err),
        };
        let key = match params.get_key() {
            Ok(k) => k.to_vec(),
            Err(err) => return Promise::err(err),
        };
        let read_lsn = params.get_read_lsn();

        let data = self.data.clone();
        Promise::from_future(async move {
            let found = data.txn_get(&key, read_lsn);
            let mut res = results.get();
            res.set_durable_lsn(data.durable_lsn());
            match found {
                Some(Some(v)) => {
                    res.set_found(true);
                    res.set_value(&v);
                }
                _ => {
                    res.set_found(false);
                }
            }
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
        let request_id = params.get_request_id();
        let records = match params.get_records() {
            Ok(r) => r,
            Err(err) => return Promise::err(err),
        };

        let mut ops: Vec<(u8, Vec<u8>, Vec<u8>)> = Vec::with_capacity(records.len() as usize);
        for rec in records.iter() {
            let op = rec.get_op();
            let key = rec.get_key().map(|k| k.to_vec()).unwrap_or_default();
            let value = rec.get_value().map(|v| v.to_vec()).unwrap_or_default();
            let mapped_op = match op {
                1 => WAL_OP_TXN_PUT,
                2 => WAL_OP_TXN_DEL,
                3 => WAL_OP_TXN_COMMIT,
                _ => {
                    return Promise::err(capnp::Error::failed(format!(
                        "invalid txn op: {op}"
                    )))
                }
            };
            ops.push((mapped_op, key, value));
        }

        let data = self.data.clone();
        Promise::from_future(async move {
            let commit_lsn = data
                .append_txn_batch_sync(request_id, ops)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            let mut res = results.get();
            res.set_commit_lsn(commit_lsn);
            res.set_durable_lsn(data.durable_lsn());
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
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::open(dir).await?);

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
