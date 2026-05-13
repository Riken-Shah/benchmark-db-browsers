// SQLite worker. Hosts @sqlite.org/sqlite-wasm and serves benchmark ops.
// Lives in a dedicated Worker so it can use the synchronous OPFS VFS.

import sqlite3InitModule from 'https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.50.4-build1/sqlite-wasm/jswasm/sqlite3.mjs';

let sqlite3 = null;
let db = null;
let poolUtil = null;
let version = null;
let mode = 'memory';
const DB_FILE = '/benchmark.sqlite3';
const POOL_DIR = 'bench-sqlite-pool';

const SCHEMA = `
  CREATE TABLE rows (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    score REAL NOT NULL,
    payload TEXT NOT NULL
  );
  CREATE INDEX idx_email ON rows(email);
`;

async function ensureSqlite() {
  if (sqlite3) return sqlite3;
  sqlite3 = await sqlite3InitModule({
    print: () => {},
    printErr: (m) => console.warn('[sqlite]', m),
  });
  version = sqlite3.version.libVersion;
  return sqlite3;
}

async function ensurePool() {
  if (poolUtil) return poolUtil;
  if (typeof sqlite3.installOpfsSAHPoolVfs !== 'function') {
    throw new Error('OPFS SAH-Pool VFS not available in this build');
  }
  poolUtil = await sqlite3.installOpfsSAHPoolVfs({
    name: 'opfs-sahpool-bench',
    directory: POOL_DIR,
    initialCapacity: 4,
  });
  return poolUtil;
}

async function openDb(nextMode) {
  if (db) { try { db.close(); } catch {} db = null; }
  await ensureSqlite();
  mode = nextMode;
  if (mode === 'persistent') {
    await ensurePool();
    db = new poolUtil.OpfsSAHPoolDb(DB_FILE);
  } else {
    db = new sqlite3.oo1.DB(':memory:');
  }
}

function resetSchema() {
  db.exec('DROP TABLE IF EXISTS rows');
  db.exec(SCHEMA);
}

function bulkWrite(rows) {
  db.exec('BEGIN');
  try {
    const stmt = db.prepare('INSERT INTO rows (id,name,email,created_at,score,payload) VALUES (?,?,?,?,?,?)');
    try {
      for (const r of rows) {
        stmt.bind([r.id, r.name, r.email, r.created_at, r.score, r.payload]);
        stmt.stepReset();
      }
    } finally {
      stmt.finalize();
    }
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  }
}

function seqRead() {
  let count = 0;
  const stmt = db.prepare('SELECT id,name,email,created_at,score,payload FROM rows ORDER BY id');
  try {
    while (stmt.step()) {
      // Materialize to make this a fair comparison with IDB/PGlite which return objects.
      stmt.get({});
      count++;
    }
  } finally {
    stmt.finalize();
  }
  return count;
}

function pointReads(ids) {
  let hit = 0;
  const stmt = db.prepare('SELECT id,name,email,created_at,score,payload FROM rows WHERE id = ?');
  try {
    for (const id of ids) {
      stmt.bind([id]);
      if (stmt.step()) {
        stmt.get({});
        hit++;
      }
      stmt.reset();
    }
  } finally {
    stmt.finalize();
  }
  return hit;
}

function indexedReads(emails) {
  let hit = 0;
  const stmt = db.prepare('SELECT id,name,email,created_at,score,payload FROM rows WHERE email = ?');
  try {
    for (const e of emails) {
      stmt.bind([e]);
      if (stmt.step()) {
        stmt.get({});
        hit++;
      }
      stmt.reset();
    }
  } finally {
    stmt.finalize();
  }
  return hit;
}

async function wipePersistent() {
  try {
    if (db) { db.close(); db = null; }
  } catch {}
  // Wipe via the pool util if available (it knows how to remove its backing files).
  try {
    if (!sqlite3) await ensureSqlite();
    await ensurePool();
    if (typeof poolUtil.unlink === 'function') {
      poolUtil.unlink(DB_FILE);
    } else if (typeof poolUtil.removeVfs === 'function') {
      await poolUtil.removeVfs();
      poolUtil = null;
    }
  } catch {}
  // Best-effort: also drop the directory in OPFS so a stuck state can recover.
  try {
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(POOL_DIR, { recursive: true }).catch(() => {});
  } catch {}
}

self.addEventListener('message', async (ev) => {
  const { id, op, args } = ev.data || {};
  try {
    let result;
    switch (op) {
      case 'init':
        await openDb(args.mode);
        result = { version, mode };
        break;
      case 'reset':
        resetSchema();
        result = true;
        break;
      case 'bulkWrite':
        bulkWrite(args.rows);
        result = args.rows.length;
        break;
      case 'seqRead':
        result = seqRead();
        break;
      case 'pointReads':
        result = pointReads(args.ids);
        break;
      case 'indexedReads':
        result = indexedReads(args.emails);
        break;
      case 'close':
        if (db) { try { db.close(); } catch {} db = null; }
        result = true;
        break;
      case 'wipePersistent':
        await wipePersistent();
        result = true;
        break;
      case 'version':
        await ensureSqlite();
        result = version;
        break;
      default:
        throw new Error('unknown op: ' + op);
    }
    self.postMessage({ id, ok: true, result });
  } catch (e) {
    self.postMessage({ id, ok: false, error: String(e && e.message || e) });
  }
});
