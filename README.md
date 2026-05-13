# Client-side database benchmark

A head-to-head comparison of three browser-native databases ([IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API), [SQLite](https://sqlite.org/wasm/), and [PGlite](https://github.com/electric-sql/pglite)) running entirely in the browser, with no server, no build step, and no dependencies beyond CDN imports.

PGlite is a newer take: real PostgreSQL compiled to WASM, running in your tab with its data dir backed by IndexedDB.

**Live demo:** https://benchmark-db-browsers.riken.me

## What it measures

Five operations across 1k / 10k / 100k rows of a realistic ~200-byte schema:

| Operation | What it tests |
|---|---|
| Load & init | Cold-start cost from `import()` to a ready, empty schema |
| Bulk write | Insert N rows in a single transaction |
| Sequential read | Read all N rows back in insertion order |
| Random point read | 100 lookups by random primary key |
| Indexed query | 100 lookups by indexed secondary column (`email`) |

Each measurement runs **1 warmup pass + 3 timed passes**, reporting the median.

## Storage backends

| Engine | Persistent storage |
|---|---|
| [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API) | Browser-native (this *is* the storage) |
| [SQLite](https://sqlite.org/wasm/) | OPFS, via the **SAH (Sync Access Handle) Pool VFS**: Sync Access Handles in a dedicated worker, no SharedArrayBuffer required |
| [PGlite](https://github.com/electric-sql/pglite) | Real Postgres compiled to WASM. Pages stored in **IndexedDB** (`idb://` driver) |

The "in-memory" toggle runs SQLite as `:memory:` and PGlite as `memory://`. IndexedDB has no in-memory mode and is shown as N/A in that mode.

## Methodology notes

- Same row data is fed to all three engines for each size, so they are working on identical inputs.
- SQLite point/indexed reads use a prepared statement bound in a loop.
- PGlite point/indexed reads use SQL-level `PREPARE`/`EXECUTE` inside an explicit transaction, with `ANALYZE rows` after bulk write so the planner has stats.
- IndexedDB point/indexed reads issue all N keyed `get()`s in a single read-only transaction.
- All engines run sequentially (IDB, then SQLite, then PGlite) to avoid sharing the same CPU thread or disk during a measurement.

## Fairness

Each engine uses its own idiomatic best path, not a one-shape-fits-all loop. The *shape* of the work differs by engine, and that asymmetry is part of what the benchmark reveals.

| Op | IndexedDB | SQLite | PGlite |
|---|---|---|---|
| Init | reopen connection | spawn worker, compile WASM, install SAH pool, open DB | recreate PGlite instance (worker, WASM, restore data dir) |
| Bulk write | single readwrite tx, batched `store.put` | `BEGIN` + prepared `INSERT` loop + `COMMIT` | `COPY FROM '/dev/blob'` then `ANALYZE` |
| Seq read | `openCursor` iterate, materialize each row | prepared `SELECT` + step loop, materialize each row | single `pg.query('SELECT ... ORDER BY id')` |
| Random point read | all 100 `get(id)` queued in one read-only tx, `Promise.all` on completion | prepared `SELECT` bound in a tight worker-side loop, no postMessage per query | `PREPARE qpt`, `BEGIN`, `EXECUTE qpt($1)` loop, `COMMIT`, `DEALLOCATE` |
| Indexed query | same pattern via `index('email')` | same pattern by `email` | same pattern by `email` |

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
- [`@sqlite.org/sqlite-wasm`](https://sqlite.org/wasm/) 3.50
- [`@electric-sql/pglite`](https://github.com/electric-sql/pglite) 0.3 (newer Postgres running as WASM)
- Source Sans 3 + JetBrains Mono via Google Fonts

## License

MIT. See [LICENSE](LICENSE).
