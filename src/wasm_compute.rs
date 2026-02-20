#[cfg(target_arch = "wasm32")]
use crate::{EmbeddedCompute, KEY_SIZE, VALUE_SIZE};
#[cfg(target_arch = "wasm32")]
use console_error_panic_hook::set_once as set_panic_hook_once;
#[cfg(target_arch = "wasm32")]
use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use tokio::sync::Mutex;
#[cfg(target_arch = "wasm32")]
use tokio::task::LocalSet;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct WasmComputeClient {
    inner: Arc<Mutex<EmbeddedCompute>>,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WasmComputeClient {
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect(storage_url: String, quorum: u32) -> Result<WasmComputeClient, JsValue> {
        set_panic_hook_once();
        let addrs = vec![storage_url];
        let local = LocalSet::new();
        let compute = EmbeddedCompute::connect(&addrs, quorum as usize, &local)
            .await
            .map_err(to_js_err)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(compute)),
        })
    }

    #[wasm_bindgen(js_name = keySize)]
    pub fn key_size() -> usize {
        KEY_SIZE
    }

    #[wasm_bindgen(js_name = valueSize)]
    pub fn value_size() -> usize {
        VALUE_SIZE
    }

    #[wasm_bindgen(js_name = putUtf8)]
    pub async fn put_utf8(&self, key: String, value: String) -> Result<u64, JsValue> {
        let key_buf = fixed_from_utf8::<KEY_SIZE>(&key, "key")?;
        let value_buf = fixed_from_utf8::<VALUE_SIZE>(&value, "value")?;
        let guard = self.inner.lock().await;
        guard.put(&key_buf, &value_buf).await.map_err(to_js_err)
    }

    #[wasm_bindgen(js_name = getUtf8)]
    pub async fn get_utf8(&self, key: String) -> Result<Option<String>, JsValue> {
        let key_buf = fixed_from_utf8::<KEY_SIZE>(&key, "key")?;
        let guard = self.inner.lock().await;
        let out = guard.get(&key_buf).await.map_err(to_js_err)?;
        Ok(out.map(|bytes| decode_zero_padded_utf8(&bytes)))
    }

    #[wasm_bindgen(js_name = deleteUtf8)]
    pub async fn delete_utf8(&self, key: String) -> Result<u64, JsValue> {
        let key_buf = fixed_from_utf8::<KEY_SIZE>(&key, "key")?;
        let guard = self.inner.lock().await;
        guard.delete(&key_buf).await.map_err(to_js_err)
    }
}

#[cfg(target_arch = "wasm32")]
fn fixed_from_utf8<const N: usize>(input: &str, field: &str) -> Result<[u8; N], JsValue> {
    let bytes = input.as_bytes();
    if bytes.len() > N {
        return Err(JsValue::from_str(&format!(
            "{field} too long: len={} max={N}",
            bytes.len()
        )));
    }
    let mut out = [0u8; N];
    out[..bytes.len()].copy_from_slice(bytes);
    Ok(out)
}

#[cfg(target_arch = "wasm32")]
fn decode_zero_padded_utf8(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

#[cfg(target_arch = "wasm32")]
fn to_js_err(err: crate::Error) -> JsValue {
    JsValue::from_str(&err.to_string())
}
