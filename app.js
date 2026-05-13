// Benchmark harness: IndexedDB vs SQLite WASM vs PGlite.
// All measurement code lives here; SQLite work is delegated to ./sqlite-worker.js.

// ---------- constants ----------

const COLORS = {
  idb:    { line: '#404040', fill: 'rgba(64,64,64,0.08)' },
  sqlite: { line: '#0d9488', fill: 'rgba(13,148,136,0.08)' },
  pglite: { line: '#d97706', fill: 'rgba(217,119,6,0.08)' },
};

const ENGINE_LABEL = { idb: 'IndexedDB', sqlite: 'SQLite WASM', pglite: 'PGlite' };
const OPS = ['init', 'write', 'readseq', 'randread', 'indexed'];
const OP_LABEL = {
  init: 'Load & init (ms)',
  write: 'Bulk write (rows/sec)',
  readseq: 'Sequential read (rows/sec)',
  randread: 'Random point read (reads/sec)',
  indexed: 'Indexed query (reads/sec)',
};

const WARMUP = 1;
const REPS = 3;
const PROBE_COUNT = 200; // number of point / indexed lookups per measured run

// ---------- utilities ----------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => performance.now();
const median = (arr) => {
  const s = [...arr].sort((a, b) => a - b);
  return s.length % 2 ? s[(s.length - 1) >> 1] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2;
};

function generateRows(n) {
  const rows = new Array(n);
  const start = Date.now();
  for (let i = 0; i < n; i++) {
    rows[i] = {
      id: crypto.randomUUID(),
      name: `User ${i}`,
      email: `u${i}_${(Math.random() * 1e9 | 0).toString(36)}@example.com`,
      created_at: start - ((Math.random() * 365 * 24 * 3600 * 1000) | 0),
      score: Math.round(Math.random() * 100000) / 100,
      payload: JSON.stringify({ i, k: 'v', flag: i % 2 === 0, t: ['a', 'b', 'c'] }),
    };
  }
  return rows;
}

function sampleField(rows, k, field) {
  const out = new Array(k);
  const n = rows.length;
  for (let i = 0; i < k; i++) out[i] = rows[(Math.random() * n) | 0][field];
  return out;
}

// ---------- IndexedDB engine ----------

const idbEngine = (() => {
  let db = null;
  const DB_NAME = 'bench_idb';
  const STORE = 'rows';

  function openDb() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const d = req.result;
        if (d.objectStoreNames.contains(STORE)) d.deleteObjectStore(STORE);
        const s = d.createObjectStore(STORE, { keyPath: 'id' });
        s.createIndex('email', 'email', { unique: false });
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }
  function closeDb() {
    if (db) { try { db.close(); } catch {} db = null; }
  }
  function deleteDb() {
    return new Promise((resolve) => {
      const req = indexedDB.deleteDatabase(DB_NAME);
      req.onsuccess = req.onerror = req.onblocked = () => resolve();
    });
  }
  function clearStore() {
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, 'readwrite');
      tx.objectStore(STORE).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  return {
    name: 'idb',
    supports(mode) { return mode === 'persistent'; },
    async init(mode) {
      closeDb();
      db = await openDb();
      return { version: 'native' };
    },
    async resetSchema() { await clearStore(); },
    async bulkWrite(rows) {
      return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readwrite');
        const s = tx.objectStore(STORE);
        for (let i = 0; i < rows.length; i++) s.put(rows[i]);
        tx.oncomplete = () => resolve(rows.length);
        tx.onerror = () => reject(tx.error);
        tx.onabort = () => reject(tx.error);
      });
    },
    async seqRead() {
      return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readonly');
        const req = tx.objectStore(STORE).openCursor();
        let count = 0;
        req.onsuccess = () => {
          const c = req.result;
          if (c) { /* materialize */ const _ = c.value; count++; c.continue(); }
          else resolve(count);
        };
        req.onerror = () => reject(req.error);
      });
    },
    async pointReads(ids) {
      return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readonly');
        const s = tx.objectStore(STORE);
        let hit = 0;
        let pending = ids.length;
        if (!pending) return resolve(0);
        for (let i = 0; i < ids.length; i++) {
          const r = s.get(ids[i]);
          r.onsuccess = () => {
            if (r.result) hit++;
            if (--pending === 0) resolve(hit);
          };
          r.onerror = () => reject(r.error);
        }
        tx.onerror = () => reject(tx.error);
      });
    },
    async indexedReads(emails) {
      return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readonly');
        const idx = tx.objectStore(STORE).index('email');
        let hit = 0;
        let pending = emails.length;
        if (!pending) return resolve(0);
        for (let i = 0; i < emails.length; i++) {
          const r = idx.get(emails[i]);
          r.onsuccess = () => {
            if (r.result) hit++;
            if (--pending === 0) resolve(hit);
          };
          r.onerror = () => reject(r.error);
        }
        tx.onerror = () => reject(tx.error);
      });
    },
    async close() { closeDb(); },
    async wipePersistent() { closeDb(); await deleteDb(); },
  };
})();

// ---------- SQLite engine (worker RPC) ----------

const sqliteEngine = (() => {
  let worker = null;
  let seq = 0;
  const pending = new Map();
  let workerVersion = null;

  function ensureWorker() {
    if (worker) return worker;
    worker = new Worker('./sqlite-worker.js', { type: 'module' });
    worker.addEventListener('message', (ev) => {
      const { id, ok, result, error } = ev.data || {};
      const slot = pending.get(id);
      if (!slot) return;
      pending.delete(id);
      ok ? slot.resolve(result) : slot.reject(new Error(error));
    });
    worker.addEventListener('error', (ev) => {
      for (const slot of pending.values()) slot.reject(new Error(ev.message || 'worker error'));
      pending.clear();
    });
    return worker;
  }
  function call(op, args) {
    const w = ensureWorker();
    const id = ++seq;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      w.postMessage({ id, op, args });
    });
  }
  function killWorker() {
    if (worker) { worker.terminate(); worker = null; }
    for (const slot of pending.values()) slot.reject(new Error('worker terminated'));
    pending.clear();
  }

  return {
    name: 'sqlite',
    supports(_mode) { return true; },
    async init(mode) {
      killWorker();
      const r = await call('init', { mode });
      workerVersion = r.version;
      return { version: r.version };
    },
    async resetSchema() { await call('reset'); },
    async bulkWrite(rows) { return call('bulkWrite', { rows }); },
    async seqRead() { return call('seqRead'); },
    async pointReads(ids) { return call('pointReads', { ids }); },
    async indexedReads(emails) { return call('indexedReads', { emails }); },
    async close() { try { await call('close'); } catch {} killWorker(); },
    async wipePersistent() {
      try { await call('wipePersistent'); } catch {}
      killWorker();
    },
    get version() { return workerVersion; },
  };
})();

// ---------- PGlite engine ----------

const pgliteEngine = (() => {
  let pg = null;
  let PGliteCtor = null;
  let pgVersion = null;

  async function ensureModule() {
    if (PGliteCtor) return PGliteCtor;
    const mod = await import('https://cdn.jsdelivr.net/npm/@electric-sql/pglite@0.3.10/dist/index.js');
    PGliteCtor = mod.PGlite;
    return PGliteCtor;
  }
  async function closePg() {
    if (pg) { try { await pg.close(); } catch {} pg = null; }
  }

  return {
    name: 'pglite',
    supports(_mode) { return true; },
    async init(mode) {
      await closePg();
      const Ctor = await ensureModule();
      const uri = mode === 'persistent' ? 'idb://bench_pglite' : 'memory://';
      pg = await Ctor.create(uri);
      // Coerce a version readout. PGlite reports the embedded Postgres version.
      try {
        const v = await pg.query('SELECT version() AS v');
        pgVersion = (v.rows && v.rows[0] && v.rows[0].v) || 'pglite';
      } catch { pgVersion = 'pglite'; }
      return { version: pgVersion };
    },
    async resetSchema() {
      await pg.exec(`
        DROP TABLE IF EXISTS rows;
        CREATE TABLE rows (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL,
          created_at BIGINT NOT NULL,
          score DOUBLE PRECISION NOT NULL,
          payload TEXT NOT NULL
        );
        CREATE INDEX idx_email ON rows(email);
      `);
    },
    async bulkWrite(rows) {
      // Use COPY FROM '/dev/blob' for PGlite's fast bulk-load path. Tab-separated text.
      const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
      const lines = new Array(rows.length);
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        lines[i] = `${esc(r.id)}\t${esc(r.name)}\t${esc(r.email)}\t${r.created_at}\t${r.score}\t${esc(r.payload)}`;
      }
      const blob = new Blob([lines.join('\n') + '\n']);
      await pg.query(`COPY rows FROM '/dev/blob' WITH (FORMAT text)`, [], { blob });
      // Give the planner stats so subsequent random reads use the indexes.
      try { await pg.exec('ANALYZE rows'); } catch {}
      return rows.length;
    },
    async seqRead() {
      const res = await pg.query('SELECT id,name,email,created_at,score,payload FROM rows ORDER BY id');
      return res.rows.length;
    },
    async pointReads(ids) {
      // Prepare once, run all 1000 inside a single explicit transaction so we don't pay
      // per-query parse+plan and implicit BEGIN/COMMIT 1000 times. Matches SQLite's
      // prepared-statement loop.
      try { await pg.exec('DEALLOCATE qpt'); } catch {}
      await pg.exec('PREPARE qpt(text) AS SELECT id,name,email,created_at,score,payload FROM rows WHERE id = $1');
      await pg.exec('BEGIN');
      let hit = 0;
      try {
        for (let i = 0; i < ids.length; i++) {
          const r = await pg.query('EXECUTE qpt($1)', [ids[i]]);
          if (r.rows.length) hit++;
        }
        await pg.exec('COMMIT');
      } catch (e) {
        try { await pg.exec('ROLLBACK'); } catch {}
        throw e;
      } finally {
        try { await pg.exec('DEALLOCATE qpt'); } catch {}
      }
      return hit;
    },
    async indexedReads(emails) {
      try { await pg.exec('DEALLOCATE qei'); } catch {}
      await pg.exec('PREPARE qei(text) AS SELECT id,name,email,created_at,score,payload FROM rows WHERE email = $1');
      await pg.exec('BEGIN');
      let hit = 0;
      try {
        for (let i = 0; i < emails.length; i++) {
          const r = await pg.query('EXECUTE qei($1)', [emails[i]]);
          if (r.rows.length) hit++;
        }
        await pg.exec('COMMIT');
      } catch (e) {
        try { await pg.exec('ROLLBACK'); } catch {}
        throw e;
      } finally {
        try { await pg.exec('DEALLOCATE qei'); } catch {}
      }
      return hit;
    },
    async close() { await closePg(); },
    async wipePersistent() {
      await closePg();
      // PGlite uses IndexedDB under idb://; delete the underlying database.
      return new Promise((resolve) => {
        const req = indexedDB.deleteDatabase('/pglite/bench_pglite');
        req.onsuccess = req.onerror = req.onblocked = () => resolve();
      });
    },
    get version() { return pgVersion; },
  };
})();

const ENGINES = { idb: idbEngine, sqlite: sqliteEngine, pglite: pgliteEngine };
const ORDER = ['idb', 'sqlite', 'pglite']; // sequential run order

// ---------- measurement primitives ----------

async function timed(fn) {
  const t0 = now();
  const out = await fn();
  return { ms: now() - t0, out };
}

async function measureRepeated(setup, run, reps, warmup) {
  const samples = [];
  for (let i = 0; i < warmup; i++) {
    if (setup) await setup();
    await run();
  }
  for (let i = 0; i < reps; i++) {
    if (setup) await setup();
    const t = await timed(run);
    samples.push(t.ms);
  }
  return {
    median: median(samples),
    min: Math.min(...samples),
    max: Math.max(...samples),
    samples,
  };
}

// ---------- chart setup ----------

const charts = {};

function fmt(v) {
  if (v == null || !isFinite(v)) return 'n/a';
  if (v === 0) return '0';
  const abs = Math.abs(v);
  if (abs >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (abs >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (abs >= 1e4) return Math.round(v / 1000) + 'k';
  if (abs >= 1e3) return (v / 1000).toFixed(1) + 'k';
  if (abs >= 100) return Math.round(v).toString();
  if (abs >= 10) return v.toFixed(1);
  if (abs >= 1) return v.toFixed(2);
  return v.toFixed(2);
}

// Inline plugin: draws the numeric value just outside each bar.
const valueLabelPlugin = {
  id: 'valueLabel',
  afterDatasetsDraw(chart) {
    const { ctx } = chart;
    const horizontal = chart.options.indexAxis === 'y';
    ctx.save();
    ctx.font = '500 11px "JetBrains Mono", ui-monospace, monospace';
    ctx.fillStyle = '#525252';
    for (let di = 0; di < chart.data.datasets.length; di++) {
      const ds = chart.data.datasets[di];
      const meta = chart.getDatasetMeta(di);
      for (let i = 0; i < meta.data.length; i++) {
        const v = ds.data[i];
        if (v == null || !isFinite(v) || v === 0) continue;
        const bar = meta.data[i];
        const label = fmt(v);
        if (horizontal) {
          ctx.textAlign = 'left';
          ctx.textBaseline = 'middle';
          ctx.fillText(label, bar.x + 6, bar.y);
        } else {
          ctx.textAlign = 'center';
          ctx.textBaseline = 'bottom';
          ctx.fillText(label, bar.x, bar.y - 4);
        }
      }
    }
    ctx.restore();
  },
};

// Subtle baseline grid styling reused across charts.
const SANS = 'Source Sans 3, system-ui, sans-serif';
const MONO = '"JetBrains Mono", ui-monospace, monospace';
const COMMON_LAYOUT = { padding: { top: 18, right: 8, bottom: 0, left: 0 } };

function makeGroupedBarDatasets() {
  return ORDER.map((eng) => ({
    label: ENGINE_LABEL[eng],
    engine: eng,
    data: [],
    backgroundColor: COLORS[eng].line,
    hoverBackgroundColor: COLORS[eng].line,
    borderRadius: 3,
    borderSkipped: false,
    barPercentage: 0.78,
    categoryPercentage: 0.78,
  }));
}

function initCharts() {
  // Init chart. Horizontal bars: lower is better, reads cleaner sideways.
  charts.init = new Chart(document.getElementById('chart-init'), {
    type: 'bar',
    data: {
      labels: ['IndexedDB', 'SQLite WASM', 'PGlite'],
      datasets: [{
        data: [null, null, null],
        backgroundColor: [COLORS.idb.line, COLORS.sqlite.line, COLORS.pglite.line],
        borderRadius: 3,
        borderSkipped: false,
        barThickness: 22,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 450, easing: 'easeOutCubic' },
      layout: { padding: { top: 6, right: 60, bottom: 6, left: 0 } },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (ctx) => `${ctx.parsed.x?.toFixed(1) ?? 'n/a'} ms` } },
      },
      scales: {
        x: {
          beginAtZero: true,
          grid: { color: '#f7f7f7', drawTicks: false },
          border: { display: false },
          ticks: { color: '#a3a3a3', font: { family: SANS, size: 11 }, callback: (v) => fmt(v) + ' ms', maxTicksLimit: 4, padding: 6 },
        },
        y: {
          grid: { display: false },
          border: { display: false },
          ticks: { color: '#404040', font: { family: SANS, size: 12.5, weight: '500' }, padding: 8 },
        },
      },
    },
    plugins: [valueLabelPlugin],
  });

  const cidMap = { write: 'chart-write', readseq: 'chart-readseq', randread: 'chart-randread', indexed: 'chart-indexed' };
  for (const op of ['write', 'readseq', 'randread', 'indexed']) {
    charts[op] = new Chart(document.getElementById(cidMap[op]), {
      type: 'bar',
      data: { labels: [], datasets: makeGroupedBarDatasets() },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 450, easing: 'easeOutCubic' },
        layout: COMMON_LAYOUT,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (items) => items.length ? `${items[0].label} rows` : '',
              label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y != null ? Math.round(ctx.parsed.y).toLocaleString() : 'n/a'}`,
            },
          },
        },
        scales: {
          x: {
            grid: { display: false },
            border: { display: false },
            ticks: { color: '#525252', font: { family: SANS, size: 12 }, padding: 6 },
          },
          y: {
            beginAtZero: true,
            grid: { color: '#f7f7f7', drawTicks: false },
            border: { display: false },
            ticks: { color: '#a3a3a3', font: { family: SANS, size: 11 }, callback: (v) => fmt(v), maxTicksLimit: 4, padding: 6 },
          },
        },
      },
      plugins: [valueLabelPlugin],
    });
  }
}

function setInitChart(results) {
  const ds = charts.init.data.datasets[0];
  ds.data = ['idb', 'sqlite', 'pglite'].map((e) => {
    const r = results[e];
    if (!r || r.unsupported) return null;
    return r.init?.median ?? null;
  });
  charts.init.update();
}

function sizeLabel(sz) {
  if (sz >= 1e6) return (sz / 1e6) + 'M';
  if (sz >= 1e3) return (sz / 1e3) + 'k';
  return String(sz);
}

function setBarChart(op, sizes, results) {
  const c = charts[op];
  c.data.labels = sizes.map(sizeLabel);
  for (const ds of c.data.datasets) {
    ds.data = sizes.map((sz) => {
      const eng = ds.engine;
      const r = results[eng]?.[op]?.[sz];
      if (!r) return null;
      const opsPerSec = (op === 'write' || op === 'readseq')
        ? (sz / r.median) * 1000
        : (PROBE_COUNT / r.median) * 1000;
      return opsPerSec;
    });
  }
  c.update();
}

// ---------- raw table ----------

function renderRawTable(results, sizes) {
  const tbody = document.querySelector('#rawtable tbody');
  const fragments = [];
  for (const eng of ORDER) {
    const r = results[eng];
    if (!r) continue;
    if (r.init) {
      fragments.push(`<tr><td class="eng-${eng}">${ENGINE_LABEL[eng]}</td><td>init</td><td>-</td><td>${r.init.median.toFixed(2)}</td><td>${r.init.min.toFixed(2)}</td><td>${r.init.max.toFixed(2)}</td><td class="muted">-</td></tr>`);
    }
    for (const op of ['write', 'readseq', 'randread', 'indexed']) {
      for (const sz of sizes) {
        const m = r[op]?.[sz];
        if (!m) continue;
        const opsPerSec = (op === 'write' || op === 'readseq')
          ? (sz / m.median) * 1000
          : (PROBE_COUNT / m.median) * 1000;
        fragments.push(`<tr><td class="eng-${eng}">${ENGINE_LABEL[eng]}</td><td>${op}</td><td>${sz.toLocaleString()}</td><td>${m.median.toFixed(2)}</td><td>${m.min.toFixed(2)}</td><td>${m.max.toFixed(2)}</td><td>${opsPerSec.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td></tr>`);
      }
    }
  }
  tbody.innerHTML = fragments.length ? fragments.join('') : '<tr><td colspan="7" class="muted">no runs yet</td></tr>';
}

// ---------- progress ----------

const progressEl = document.getElementById('progress');
function setProgress(text, pct) {
  if (!text) {
    progressEl.className = 'progress idle';
    progressEl.innerHTML = 'idle. click <em>Run benchmark</em> to begin';
    return;
  }
  progressEl.className = 'progress';
  const safe = text.replace(/</g, '&lt;');
  if (pct == null) progressEl.textContent = safe;
  else progressEl.innerHTML = `${safe} <span class="bar"><i style="width:${(pct * 100).toFixed(1)}%"></i></span>`;
}

// ---------- main run ----------

let running = false;
let totalSteps = 0;
let stepIdx = 0;

function bumpProgress(text) {
  stepIdx++;
  setProgress(text, stepIdx / totalSteps);
}

async function runAll() {
  if (running) return;
  running = true;
  document.getElementById('run').disabled = true;

  const mode = document.querySelector('input[name="mode"]:checked').value;
  const sizes = [...document.querySelectorAll('.sizes input:checked')]
    .map((el) => +el.value)
    .sort((a, b) => a - b);

  if (!sizes.length) {
    setProgress('select at least one dataset size', 0);
    running = false;
    document.getElementById('run').disabled = false;
    return;
  }

  const results = {};
  const versions = {};

  // 1 init benchmark per engine + (4 ops × sizes) measured groups.
  // Each measured group is (WARMUP + REPS) runs of the op. Steps for progress = engines * (1 init group + ops*sizes).
  const enginesToRun = ORDER.filter((e) => ENGINES[e].supports(mode));
  totalSteps = enginesToRun.length * (1 + 4 * sizes.length);
  stepIdx = 0;

  // Pre-generate row sets per size so all engines hit identical data.
  setProgress('generating row sets…', 0);
  const dataBySize = {};
  for (const sz of sizes) {
    const rows = generateRows(sz);
    dataBySize[sz] = {
      rows,
      probeIds: sampleField(rows, PROBE_COUNT, 'id'),
      probeEmails: sampleField(rows, PROBE_COUNT, 'email'),
    };
  }
  await sleep(20);

  for (const eng of ORDER) {
    if (!ENGINES[eng].supports(mode)) {
      results[eng] = { unsupported: true };
      continue;
    }
    const e = ENGINES[eng];
    results[eng] = {};

    // --- init benchmark: full open + close cycles ---
    bumpProgress(`${ENGINE_LABEL[eng]} · cold init`);
    const initSamples = [];
    for (let i = 0; i < WARMUP; i++) {
      await e.init(mode);
      await e.close();
    }
    for (let i = 0; i < REPS; i++) {
      const t = await timed(() => e.init(mode));
      initSamples.push(t.ms);
      // Capture version on last run, keep DB open for subsequent ops.
      if (i === REPS - 1) versions[eng] = e.version || 'unknown';
      else await e.close();
    }
    results[eng].init = {
      median: median(initSamples),
      min: Math.min(...initSamples),
      max: Math.max(...initSamples),
      samples: initSamples,
    };
    setInitChart(results);

    // --- per-size benchmarks ---
    for (const sz of sizes) {
      const { rows, probeIds, probeEmails } = dataBySize[sz];
      results[eng].write = results[eng].write || {};
      results[eng].readseq = results[eng].readseq || {};
      results[eng].randread = results[eng].randread || {};
      results[eng].indexed = results[eng].indexed || {};

      // bulk write: schema reset between reps
      bumpProgress(`${ENGINE_LABEL[eng]} · bulk write @ ${sz.toLocaleString()}`);
      results[eng].write[sz] = await measureRepeated(
        async () => { await e.resetSchema(); },
        async () => { await e.bulkWrite(rows); },
        REPS, WARMUP,
      );
      setBarChart('write', sizes, results);

      // Ensure the table has data for read benchmarks
      await e.resetSchema();
      await e.bulkWrite(rows);

      // sequential read
      bumpProgress(`${ENGINE_LABEL[eng]} · sequential read @ ${sz.toLocaleString()}`);
      results[eng].readseq[sz] = await measureRepeated(
        null,
        async () => { await e.seqRead(); },
        REPS, WARMUP,
      );
      setBarChart('readseq', sizes, results);

      // random point read
      bumpProgress(`${ENGINE_LABEL[eng]} · random point read @ ${sz.toLocaleString()}`);
      results[eng].randread[sz] = await measureRepeated(
        null,
        async () => { await e.pointReads(probeIds); },
        REPS, WARMUP,
      );
      setBarChart('randread', sizes, results);

      // indexed query
      bumpProgress(`${ENGINE_LABEL[eng]} · indexed query @ ${sz.toLocaleString()}`);
      results[eng].indexed[sz] = await measureRepeated(
        null,
        async () => { await e.indexedReads(probeEmails); },
        REPS, WARMUP,
      );
      setBarChart('indexed', sizes, results);

      renderRawTable(results, sizes);
    }

    await e.close();
  }

  renderRawTable(results, sizes);
  setProgress(`done at ${new Date().toLocaleTimeString()}`, 1);
  setTimeout(() => setProgress(null), 4000);
  renderVersions(versions);

  running = false;
  document.getElementById('run').disabled = false;
}

// ---------- environment / versions ----------

function renderEnv() {
  const ua = navigator.userAgent;
  let browser = 'Unknown';
  if (/Edg\//.test(ua)) browser = 'Edge';
  else if (/Chrome\//.test(ua) && !/Edg\//.test(ua)) browser = 'Chrome';
  else if (/Firefox\//.test(ua)) browser = 'Firefox';
  else if (/Safari\//.test(ua) && /Version\//.test(ua)) browser = 'Safari';
  const m = ua.match(new RegExp(`${browser}/(\\d+)`));
  const ver = m ? m[1] : '';
  const platform = /Mac/.test(ua) ? 'macOS' : /Win/.test(ua) ? 'Windows' : /Linux/.test(ua) ? 'Linux' : /Android/.test(ua) ? 'Android' : /iPhone|iPad/.test(ua) ? 'iOS' : 'Unknown';
  const cores = navigator.hardwareConcurrency || '?';
  const mem = navigator.deviceMemory ? `${navigator.deviceMemory}GB` : '?';
  document.getElementById('env').textContent = `${browser} ${ver} · ${platform} · ${cores} cores · ${mem} RAM`;
}

function renderVersions(versions) {
  const parts = [];
  if (versions.idb) parts.push(`idb: ${versions.idb}`);
  if (versions.sqlite) parts.push(`sqlite: ${versions.sqlite}`);
  if (versions.pglite) {
    const m = String(versions.pglite).match(/PostgreSQL\s+(\d+\.\d+)/i);
    parts.push(`pglite: pg ${m ? m[1] : versions.pglite}`);
  }
  document.getElementById('versions').textContent = parts.join(' · ') || ' ';
}

// ---------- wire up ----------

document.getElementById('run').addEventListener('click', runAll);
document.getElementById('reset').addEventListener('click', async () => {
  if (running) return;
  if (!confirm('Wipe all persistent benchmark storage (OPFS + IndexedDB)?')) return;
  setProgress('clearing persistent storage…');
  for (const eng of ORDER) {
    try { await ENGINES[eng].wipePersistent(); } catch {}
  }
  setProgress('cleared. ready for a fresh run', 1);
  setTimeout(() => setProgress(null), 2500);
});

renderEnv();
initCharts();
setProgress(null);
