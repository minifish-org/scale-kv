use crate::storage_capnp::storage;
use crate::{Result, StorageNode};
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::RpcSystem;
use std::net::SocketAddr;
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
    fn get(
        &mut self,
        params: storage::GetParams,
        mut results: storage::GetResults,
    ) -> Promise<(), capnp::Error> {
        let key = match params.get().and_then(|p| p.get_key()) {
            Ok(key) => match key.to_str() {
                Ok(key) => key.to_string(),
                Err(err) => return Promise::err(capnp::Error::failed(err.to_string())),
            },
            Err(err) => return Promise::err(err),
        };

        if let Some(value) = self.data.get(&key) {
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
        let key = match params.get_key() {
            Ok(key) => match key.to_str() {
                Ok(key) => key.to_string(),
                Err(err) => return Promise::err(capnp::Error::failed(err.to_string())),
            },
            Err(err) => return Promise::err(err),
        };
        let value = match params.get_value() {
            Ok(value) => value.to_vec(),
            Err(err) => return Promise::err(err),
        };

        self.data.put(&key, &value);
        Promise::ok(())
    }

    fn delete(
        &mut self,
        params: storage::DeleteParams,
        mut results: storage::DeleteResults,
    ) -> Promise<(), capnp::Error> {
        let key = match params.get().and_then(|p| p.get_key()) {
            Ok(key) => match key.to_str() {
                Ok(key) => key.to_string(),
                Err(err) => return Promise::err(capnp::Error::failed(err.to_string())),
            },
            Err(err) => return Promise::err(err),
        };
        let existed = self.data.get(&key).is_some();
        self.data.delete(&key);
        results.get().set_found(existed);
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
