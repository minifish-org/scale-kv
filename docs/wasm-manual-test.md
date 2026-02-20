# WASM Compute Manual Test

This runs `EmbeddedCompute` in browser WASM and uses `put/get/delete` from JS.

## 1. Start storage server

```bash
cd /Users/yusp/work/scale-kv
cargo run --bin storage_server -- --addr 127.0.0.1:4001 --dir /tmp/scale-kv-wasm-manual
```

## 2. Build wasm package

```bash
cd /Users/yusp/work/scale-kv
cargo install wasm-pack
wasm-pack build --target web --out-dir wasm-pkg
```

## 3. Serve static files

Any static server is fine. Example:

```bash
cd /Users/yusp/work/scale-kv
python3 -m http.server 8080
```

## 4. Open test page

Open:

`http://127.0.0.1:8080/examples/wasm-compute-test.html`

Workflow:
- Click `Connect WASM Compute`
- Use `putUtf8`
- Verify with `getUtf8`
- Optionally `deleteUtf8` then `getUtf8` again

Notes:
- `KEY_SIZE` is 16 bytes, `VALUE_SIZE` is 1024 bytes.
- The wrapper uses UTF-8 and zero-padding to fit fixed sizes.
