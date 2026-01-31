use crate::storage_capnp::{storage, stream};
use crate::{PageId, Result, StorageNode};
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
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

struct StreamService {
    data: Arc<StorageNode>,
    keys: Vec<PageId>,
    position: usize,
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

        if let Some(value) = self.data.get(key) {
            let mut res = results.get();
            res.set_found(true);
            res.set_value(&value);
        } else {
            results.get().set_found(false);
        }

        Promise::ok(())
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

        self.data.put(key, &value);
        Promise::ok(())
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
        let existed = self.data.get(key).is_some();
        self.data.delete(key);
        results.get().set_found(existed);
        Promise::ok(())
    }

    fn stream(
        &mut self,
        _params: storage::StreamParams,
        mut results: storage::StreamResults,
    ) -> Promise<(), capnp::Error> {
        let keys = self.data.keys();
        let client: stream::Client = capnp_rpc::new_client(StreamService {
            data: self.data.clone(),
            keys,
            position: 0,
        });
        results.get().set_stream(client);
        Promise::ok(())
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

        for item in items.iter() {
            let key = item.get_key();
            let value = match item.get_value() {
                Ok(value) => value.to_vec(),
                Err(err) => return Promise::err(err),
            };
            self.data.put(key, &value);
        }

        Promise::ok(())
    }

}

impl stream::Server for StreamService {
    fn next(
        &mut self,
        params: stream::NextParams,
        mut results: stream::NextResults,
    ) -> Promise<(), capnp::Error> {
        let max = match params.get() {
            Ok(params) => params.get_max() as usize,
            Err(err) => return Promise::err(err),
        };
        let remaining = self.keys.len().saturating_sub(self.position);
        let count = remaining.min(max);
        let mut batch = Vec::with_capacity(count);
        for key in self.keys[self.position..self.position + count].iter() {
            if let Some(value) = self.data.get(*key) {
                batch.push((*key, value));
            }
        }
        self.position += count;
        let mut list = results.get().init_items(batch.len() as u32);
        for (i, (key, value)) in batch.into_iter().enumerate() {
            let mut item = list.reborrow().get(i as u32);
            item.set_key(key);
            item.set_value(&value);
        }
        results.get().set_done(self.position >= self.keys.len());
        Promise::ok(())
    }
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
