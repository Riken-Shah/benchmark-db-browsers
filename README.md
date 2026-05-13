# Client-side database benchmark

A head-to-head comparison of three browser-native databases (IndexedDB, SQLite WASM, and PGlite) running entirely in the browser, with no server, no build step, and no dependencies beyond CDN imports.

**Live demo:** https://benchmark-db-browsers.riken.me

## What it measures

Five operations across 1k / 10k / 100k rows of a realistic ~200-byte schema:

| Operation | What it tests |
|---|---|
| Load & init | Cold-start cost from `import()` to a ready, empty schema |
| Bulk write | Insert N rows in a single transaction |
| Sequential read | Read all N rows back in insertion order |
| Random point read | 200 lookups by random primary key |
| Indexed query | 200 lookups by indexed secondary column (`email`) |

Each measurement runs **1 warmup pass + 3 timed passes**, reporting the median.

## Storage backends

| Engine | Persistent storage |
|---|---|
| IndexedDB | Browser-native (this *is* the storage) |
| SQLite WASM | OPFS, via the **SAH (Sync Access Handle) Pool VFS**: Sync Access Handles in a dedicated worker, no SharedArrayBuffer required |
| PGlite | Real Postgres compiled to WASM. Pages stored in **IndexedDB** (`idb://` driver) |

The "in-memory" toggle runs SQLite as `:memory:` and PGlite as `memory://`. IndexedDB has no in-memory mode and is shown as N/A in that mode.

## Methodology notes

- Same row data is fed to all three engines for each size, so they are working on identical inputs.
- SQLite point/indexed reads use a prepared statement bound in a loop.
- PGlite point/indexed reads use SQL-level `PREPARE`/`EXECUTE` inside an explicit transaction, with `ANALYZE rows` after bulk write so the planner has stats.
- IndexedDB point/indexed reads issue all N keyed `get()`s in a single read-only transaction.
- All engines run sequentially (IDB, then SQLite, then PGlite) to avoid sharing the same CPU thread or disk during a measurement.

PGlite is fundamentally slower than the other two at random point reads. It's the full Postgres engine traversing a B-tree backed by async IndexedDB pages, where SQLite uses purpose-built WASM and IDB uses a single keyed lookup. That gap is honest, not a bug.

## Run it locally

```bash
git clone https://github.com/Riken-Shah/benchmark-db-browsers.git
cd benchmark-db-browsers
python3 -m http.server 4173
# open http://localhost:4173
```

Any static file server works. There is no build step. The site needs to be served over HTTP (not opened as `file://`) because the SQLite worker imports a WASM module.

## Tech stack

- Vanilla HTML + ES modules + a single Web Worker for SQLite
- [Chart.js](https://www.chartjs.org/) 4 for visualization
- [`@sqlite.org/sqlite-wasm`](https://www.npmjs.com/package/@sqlite.org/sqlite-wasm) 3.50
- [`@electric-sql/pglite`](https://pglite.dev) 0.3
- Source Sans 3 + JetBrains Mono via Google Fonts

## License

MIT. See [LICENSE](LICENSE).
