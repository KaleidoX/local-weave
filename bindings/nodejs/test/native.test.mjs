import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { open } from '../index.js';
// C1/N10 below characterizes existing behavior through the actual native public API.
export const schema = {
  version: 1,
  tables: [
    {
      id: 1,
      name: 'items',
      columns: [
        { name: 'id', kind: 'Text', nullable: false },
        { name: 'body', kind: 'Text', nullable: true },
      ],
    },
  ],
};
export const options = (root, device = 'a', s = schema) => ({
  path: join(root, `${device}.db`),
  identity: ['node-dataset', device, 'first'],
  schema: s,
  backend: { kind: 'Fs', root: join(root, 'remote') },
});
export const text = (value) => ({ type: 'text', value });
const put = (id, body) => ({
  sql: 'INSERT INTO items VALUES (?,?)',
  params: [text(id), text(body)],
});
test('N1 actual native Session-backed atomic write and read', async () => {
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let db;
  try {
    db = await open(options(root));
    const result = await db.writeTransaction([put('one', 'native')]);
    assert.equal(result.committed, true);
    assert.deepEqual(await db.read('SELECT * FROM items'), [[text('one'), text('native')]]);
    assert.equal((await db.status()).pendingOutbox, '1');
  } finally {
    if (db) await db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N2 typed i64/f64/blob/null, rollback, guarded SQL and durable reopen', async () => {
  const { value, decodeValue } = await import('../index.js');
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  const typedSchema = {
    version: 1,
    tables: [
      {
        id: 1,
        name: 'items',
        columns: [
          { name: 'id', kind: 'Text', nullable: false },
          { name: 'n', kind: 'Integer', nullable: true },
          { name: 'r', kind: 'Real', nullable: true },
          { name: 'b', kind: 'Blob', nullable: true },
          { name: 't', kind: 'Text', nullable: true },
        ],
      },
    ],
  };
  let db;
  try {
    db = await open(options(root, 'typed', typedSchema));
    const row = [
      value.text('one'),
      value.integer(9223372036854775807n),
      value.real(1.125),
      value.blob(Buffer.from([0, 255, 128])),
      value.null(),
    ];
    await db.writeTransaction([{ sql: 'INSERT INTO items VALUES (?,?,?,?,?)', params: row }]);
    assert.deepEqual(await db.read('SELECT * FROM items'), [row]);
    assert.equal(decodeValue(row[1]), 9223372036854775807n);
    assert.equal(decodeValue(row[2]), 1.125);
    assert.deepEqual(decodeValue(row[3]), Buffer.from([0, 255, 128]));
    assert.equal(decodeValue(row[4]), null);
    assert.throws(() => value.integer(9007199254740992), { code: 'Invalid' });
    assert.throws(() => value.real(Infinity), { code: 'Invalid' });
    const insert = {
      sql: 'INSERT INTO items VALUES (?,?,?,?,?)',
      params: [text('rollback'), ...row.slice(1)],
    };
    await assert.rejects(db.writeTransaction([insert, insert]), { code: 'Constraint' });
    await assert.rejects(db.writeTransaction([insert, { sql: 'PRAGMA user_version=3' }]), {
      code: 'Forbidden',
    });
    assert.equal((await db.status()).pendingOutbox, '1');
    for (const sql of [
      'COMMIT',
      'ATTACH DATABASE ? AS other',
      'DROP TABLE items',
      'UPDATE _lfs_state SET next_seq=1',
      "INSERT INTO items(id) VALUES ('evil'); DELETE FROM items",
    ]) {
      await assert.rejects(
        db.writeTransaction([
          { sql, params: sql.startsWith('ATTACH') ? [text(join(root, 'secret.db'))] : [] },
        ]),
        (e) => ['Forbidden', 'Invalid'].includes(e.code) && !e.message.includes(root),
      );
    }
    for (const p of [
      42,
      { type: 'integer', value: 9007199254740992 },
      { type: 'integer', value: '9223372036854775808' },
      { type: 'real', value: '7ff0000000000000' },
      { type: 'blob', value: 'gg' },
    ]) {
      await assert.rejects(db.read('SELECT ?', [p]), { code: 'Invalid' });
    }
    await assert.rejects(db.read('SELECT ?', [text('x'.repeat(524289))]), { code: 'Invalid' });
    await db.close();
    db = await open(options(root, 'typed', typedSchema));
    assert.deepEqual(await db.read('SELECT * FROM items'), [row]);
    assert.equal((await db.status()).pendingOutbox, '1');
    await assert.rejects(
      open({ ...options(root, 'invalid'), schema: { version: 1, tables: [] } }),
      { code: 'Schema' },
    );
    await assert.rejects(
      open({ ...options(root, 'invalid'), path: join(root, 'x') + '/../bad.db' }),
      { code: 'Invalid' },
    );
  } finally {
    if (db) await db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N3 two native devices Fs delete/edit conflict, observed resolve and no echo', async () => {
  const { mkdir } = await import('node:fs/promises');
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let a, b;
  try {
    await mkdir(join(root, 'remote'));
    a = await open(options(root, 'a'));
    b = await open(options(root, 'b'));
    await a.writeTransaction([put('one', 'initial')]);
    await a.syncOnce();
    await b.syncOnce();
    assert.equal((await b.status()).pendingOutbox, '0');
    await a.writeTransaction([{ sql: 'DELETE FROM items WHERE id=?', params: [text('one')] }]);
    await b.writeTransaction([
      { sql: 'UPDATE items SET body=? WHERE id=?', params: [text('edited'), text('one')] },
    ]);
    await a.syncOnce();
    await b.syncOnce();
    await a.syncOnce();
    const ca = await a.listConflicts(),
      cb = await b.listConflicts();
    assert.deepEqual(ca, cb);
    assert.equal(ca.length, 1);
    assert.equal(ca[0].heads.length, 2);
    assert.ok(ca[0].heads.some((h) => h.row === null));
    assert.ok(ca[0].heads.some((h) => h.row?.[1].value === 'edited'));
    await assert.rejects(
      a.writeTransaction([
        { sql: 'UPDATE items SET body=? WHERE id=?', params: [text('implicit'), text('one')] },
      ]),
      { code: 'ConflictRequiresResolution' },
    );
    assert.equal((await a.status()).pendingOutbox, '0');
    await a.resolve({
      table: 1,
      key: 'one',
      observed: ca[0].heads.map((h) => h.id),
      row: [text('one'), text('merged')],
    });
    await a.syncOnce();
    await b.syncOnce();
    assert.deepEqual(await a.listConflicts(), []);
    assert.deepEqual(await b.listConflicts(), []);
    assert.deepEqual(await a.read('SELECT * FROM items'), await b.read('SELECT * FROM items'));
    assert.equal((await b.status()).pendingOutbox, '0');
    assert.ok((await a.events()).some((e) => e.operation === 'sync' && e.success));
  } finally {
    if (a) await a.close();
    if (b) await b.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N4 streamed attachment hash/install/download and cooperative cancellation state', async () => {
  const { mkdir, writeFile, readFile } = await import('node:fs/promises');
  const { createHash } = await import('node:crypto');
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let a, b;
  try {
    await mkdir(join(root, 'remote'));
    a = await open(options(root, 'a'));
    b = await open(options(root, 'b'));
    const bytes = Buffer.alloc(2 * 1024 * 1024, 37),
      file = join(root, 'input');
    await writeFile(file, bytes);
    const attachment = await a.addAttachment(file);
    assert.equal(attachment.hash, createHash('sha256').update(bytes).digest('hex'));
    assert.equal(attachment.size, String(bytes.length));
    await a.writeTransaction([put('attached', 'file')], [attachment.hash]);
    await a.syncOnce();
    await b.syncOnce();
    assert.notEqual((await b.attachmentStatus(attachment.hash)).state, 'Available');
    const cancelled = new AbortController();
    cancelled.abort();
    await assert.rejects(b.downloadAttachment(attachment.hash, { signal: cancelled.signal }), {
      code: 'Cancelled',
    });
    assert.notEqual((await b.attachmentStatus(attachment.hash)).state, 'Available');
    await b.downloadAttachment(attachment.hash);
    assert.equal((await b.attachmentStatus(attachment.hash)).state, 'Available');
    const output = join(root, 'output');
    await b.exportAttachment(attachment.hash, output);
    assert.deepEqual(await readFile(output), bytes);
    await assert.rejects(a.addAttachment(file, { signal: cancelled.signal }), {
      code: 'Cancelled',
    });
    await assert.rejects(b.downloadAttachment('../escape'), { code: 'Invalid' });
    await assert.rejects(a.addAttachment(root + '/../escape'), { code: 'Invalid' });
    const large = join(root, 'large');
    await writeFile(large, Buffer.alloc(32 * 1024 * 1024, 71));
    const h = createHash('sha256')
      .update(Buffer.alloc(32 * 1024 * 1024, 71))
      .digest('hex');
    const ac = new AbortController();
    const pending = a.addAttachment(large, { signal: ac.signal });
    const timer = setTimeout(() => ac.abort(), 5);
    await assert.rejects(pending, { code: 'Cancelled' });
    clearTimeout(timer);
    assert.notEqual((await a.attachmentStatus(h)).state, 'Available');
    assert.equal((await a.status()).pendingOutbox, '0');
  } finally {
    if (a) await a.close();
    if (b) await b.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N5 bounded admission, duplicate sync Busy, FIFO writer, draining close and real loop progress', async () => {
  const { mkdir } = await import('node:fs/promises');
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let a;
  try {
    await mkdir(join(root, 'remote'));
    a = await open(options(root));
    await a.writeTransaction(Array.from({ length: 400 }, (_, i) => put(`id${i}`, 'load')));
    let ticks = 0;
    const timer = setInterval(() => ticks++, 1);
    timer.unref();
    const heavy = a.read('SELECT count(*) FROM items a, items b, items c');
    const writes = Array.from({ length: 15 }, (_, i) =>
      a.writeTransaction([
        { sql: 'UPDATE items SET body=? WHERE id=?', params: [text(`v${i}`), text('id0')] },
      ]),
    );
    const full = a.status();
    await assert.rejects(full, { code: 'Busy' });
    // Close has its own reserved slot and drains every already accepted job.
    const closing = a.close();
    await assert.rejects(a.status(), { code: 'Closed' });
    assert.deepEqual(await heavy, [[{ type: 'integer', value: '64000000' }]]);
    assert.ok(ticks > 2, `actual native query must allow JS timers, got ${ticks}`);
    clearInterval(timer);
    await Promise.all(writes);
    await closing;
    a = null;
    a = await open(options(root));
    assert.deepEqual(await a.read('SELECT body FROM items WHERE id=?', [text('id0')]), [
      [text('v14')],
    ]);
    const syncing = a.syncOnce();
    await assert.rejects(a.syncOnce(), { code: 'Busy' });
    await syncing;
    assert.equal((await a.status()).pendingOutbox, '0');
    console.log(`real native 64M-row count: event-loop timer ticks=${ticks}`);
  } finally {
    if (a) await a.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N6 shared published historical privacy closure, local pending restoration and rotated incarnation', async () => {
  const { mkdir, writeFile, readFile, readdir } = await import('node:fs/promises');
  const { restore } = await import('../index.js');
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let a, clone, local;
  try {
    await mkdir(join(root, 'remote'));
    a = await open(options(root));
    await writeFile(join(root, 'public'), 'published history');
    await writeFile(join(root, 'private'), 'PRIVATE NEVER PUBLISHED');
    const pub = await a.addAttachment(join(root, 'public')),
      priv = await a.addAttachment(join(root, 'private'));
    await a.writeTransaction([put('history', 'ancestor')], [pub.hash]);
    await a.syncOnce();
    await a.writeTransaction([{ sql: 'DELETE FROM items WHERE id=?', params: [text('history')] }]);
    await a.syncOnce();
    const sourceBefore = await readFile(options(root).path);
    const shared = join(root, 'shared');
    const manifest = await a.backup(shared, 'SharedBootstrap');
    assert.deepEqual(
      manifest.files.map((f) => f.path).sort(),
      [`objects/${pub.hash}`, 'store.db'].sort(),
    );
    assert.deepEqual(await readFile(options(root).path), sourceBefore);
    async function scan(path) {
      for (const entry of await readdir(path, { withFileTypes: true })) {
        const p = join(path, entry.name);
        if (entry.isDirectory()) await scan(p);
        else {
          const bytes = await readFile(p);
          assert.equal(bytes.includes(Buffer.from(priv.hash)), false);
          assert.equal(bytes.includes(Buffer.from('PRIVATE NEVER PUBLISHED')), false);
        }
      }
    }
    await scan(shared);
    clone = await restore({
      backup: shared,
      destination: join(root, 'clone'),
      backend: options(root).backend,
    });
    assert.equal((await clone.attachmentStatus(pub.hash)).state, 'Available');
    assert.notEqual((await clone.attachmentStatus(priv.hash)).state, 'Available');
    assert.deepEqual(await clone.read('SELECT * FROM items'), []);
    await clone.syncOnce();
    assert.deepEqual(await clone.read('SELECT * FROM items'), []);
    await a.writeTransaction([put('pending', 'offline')], [priv.hash]);
    const pending = await a.status();
    assert.equal(pending.pendingOutbox, '1');
    await assert.rejects(a.backup(join(root, 'invalidshared'), 'SharedBootstrap'), {
      code: 'Blocked',
    });
    await a.backup(join(root, 'local'), 'LocalDisaster');
    local = await restore({
      backup: join(root, 'local'),
      destination: join(root, 'restored'),
      backend: options(root).backend,
    });
    assert.equal((await local.status()).pendingOutbox, '1');
    assert.notEqual((await local.info()).identity[2], (await a.info()).identity[2]);
    assert.equal((await local.attachmentStatus(priv.hash)).state, 'Available');
    await local.writeTransaction([put('new', 'rotated')]);
    await local.syncOnce();
    await clone.syncOnce();
    assert.deepEqual(await clone.read('SELECT * FROM items ORDER BY id'), [
      [text('new'), text('rotated')],
      [text('pending'), text('offline')],
    ]);
    assert.equal((await a.status()).pendingOutbox, '1');
    async function remoteBytes(path) {
      const result = {};
      for (const entry of await readdir(path, { withFileTypes: true })) {
        const p = join(path, entry.name);
        if (entry.isDirectory()) Object.assign(result, await remoteBytes(p));
        else result[p] = await readFile(p);
      }
      return result;
    }
    const remoteBefore = await remoteBytes(join(root, 'remote'));
    await a.syncOnce();
    assert.deepEqual(await remoteBytes(join(root, 'remote')), remoteBefore);
  } finally {
    if (a) await a.close();
    if (clone) await clone.close();
    if (local) await local.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N7 unsupported schema semantics fail closed and errors stay sanitized', async () => {
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let db;
  try {
    const unsupported = structuredClone(schema);
    unsupported.tables[0].columns[1].unique = true;
    await assert.rejects(open(options(root, 'extra', unsupported)), { code: 'Invalid' });
    db = await open(options(root));
    await assert.rejects(open(options(root)), { code: 'Busy' });
    await assert.rejects(
      db.read('SELECT ?', [{ type: 'text', value: 'hidden-token', extra: true }]),
      { code: 'Invalid' },
    );
    await assert.rejects(
      db.read('SELECT ?', [{ type: 'integer', value: '1'.repeat(9 * 1024 * 1024) }]),
      { code: 'Limit' },
    );
    const events = await db.events();
    assert.equal(JSON.stringify(events).includes('hidden-token'), false);
    assert.equal(JSON.stringify(events).includes(root), false);
  } finally {
    if (db) await db.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N8 backend traversal is rejected before creating a database', async () => {
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  let unexpected;
  try {
    const promise = open({
      ...options(root),
      backend: { kind: 'Fs', root: root + '/../outside' },
    }).then((db) => {
      unexpected = db;
      return db;
    });
    await assert.rejects(promise, { code: 'Invalid' });
  } finally {
    if (unexpected) await unexpected.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('N9 process-wide handle bound and pre-cancelled native sync preserve pending work', async () => {
  const { mkdir } = await import('node:fs/promises');
  const root = await mkdtemp(join(tmpdir(), 'lfs-node-'));
  const handles = [];
  try {
    await mkdir(join(root, 'remote'));
    for (let i = 0; i < 16; i++) handles.push(await open(options(root, `device${i}`)));
    await assert.rejects(open(options(root, 'overflow')), { code: 'Busy' });
    const a = handles[0];
    await a.writeTransaction([put('pending', 'stay local')]);
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(a.syncOnce({ signal: ac.signal }), { code: 'Cancelled' });
    assert.equal((await a.status()).pendingOutbox, '1');
    await assert.rejects(
      a.writeTransaction([put('cancelled', 'not committed')], [], { signal: ac.signal }),
      { code: 'Cancelled' },
    );
    assert.deepEqual(await a.read('SELECT * FROM items'), [[text('pending'), text('stay local')]]);
  } finally {
    await Promise.all(handles.map((h) => h.close()));
    await rm(root, { recursive: true, force: true });
  }
});

test(
  'N10 C1 characterization: Missing explicit refs reject, offline edit retains historical attachment on third peer',
  { timeout: 30000 },
  async () => {
    const { mkdir, writeFile, readFile, rename } = await import('node:fs/promises');
    const { createHash } = await import('node:crypto');
    const root = await mkdtemp(join(tmpdir(), 'lfs-node-c1-'));
    let a, b, c;
    const attachedSchema = {
      version: 1,
      tables: [
        {
          id: 1,
          name: 'items',
          columns: [
            { name: 'id', kind: 'Text', nullable: false },
            { name: 'body', kind: 'Text', nullable: true },
            { name: 'file_hash', kind: 'Text', nullable: true },
          ],
        },
      ],
    };
    const opts = (device) => options(root, device, attachedSchema);
    const edit = (body) => [
      { sql: 'UPDATE items SET body=? WHERE id=?', params: [text(body), text('one')] },
    ];
    try {
      await mkdir(join(root, 'remote'));
      a = await open(opts('a'));
      b = await open(opts('b'));
      const bytes = Buffer.from([0, 255, 128, ...Buffer.from('C1 historical attachment\n')]);
      await writeFile(join(root, 'input'), bytes);
      const { hash } = await a.addAttachment(join(root, 'input'));
      assert.equal(hash, createHash('sha256').update(bytes).digest('hex'));
      const initial = [text('one'), text('initial'), text(hash)];
      await a.writeTransaction(
        [{ sql: 'INSERT INTO items VALUES (?,?,?)', params: initial }],
        [hash],
      );
      assert.equal((await a.syncOnce()).healthy, true);
      assert.equal((await b.syncOnce()).healthy, true);
      assert.deepEqual(await b.read('SELECT * FROM items'), [initial]);
      const missing = await b.attachmentStatus(hash),
        before = await b.status();
      assert.equal(missing.state, 'Missing');
      assert.equal(before.pendingOutbox, '0');
      await a.close();
      a = null;
      // Remove the owned Fs endpoint while editing; the original causal packet stays intact.
      await rename(join(root, 'remote'), join(root, 'disconnected'));
      await assert.rejects(b.writeTransaction(edit('must not commit'), [hash]), {
        code: 'Invalid',
      });
      assert.deepEqual(await b.read('SELECT * FROM items'), [initial]);
      assert.deepEqual(await b.status(), before);
      assert.deepEqual(await b.attachmentStatus(hash), missing);
      assert.deepEqual(await b.listConflicts(), []);
      console.log(
        'C1 Missing explicit reference: Invalid; business/status/attachment state unchanged',
      );
      assert.equal((await b.writeTransaction(edit('offline'))).committed, true);
      const edited = [text('one'), text('offline'), text(hash)];
      assert.deepEqual(await b.read('SELECT * FROM items'), [edited]);
      assert.equal((await b.status()).pendingOutbox, '1');
      await b.close();
      b = null;
      b = await open(opts('b'));
      assert.deepEqual(await b.read('SELECT * FROM items'), [edited]);
      assert.equal((await b.status()).pendingOutbox, '1');
      assert.deepEqual(await b.attachmentStatus(hash), missing);
      console.log(
        'C1 disconnected edit without explicit refs: committed; row and pending survive close/reopen; file Missing',
      );
      await rename(join(root, 'disconnected'), join(root, 'remote'));
      const published = await b.syncOnce();
      assert.equal(published.healthy, true);
      assert.equal(published.status.pendingOutbox, '0');
      assert.deepEqual(await b.attachmentStatus(hash), missing);
      c = await open(opts('c'));
      const received = await c.syncOnce();
      assert.equal(received.healthy, true);
      assert.equal(received.downloaded, '2');
      assert.deepEqual(await c.read('SELECT * FROM items'), [edited]);
      assert.deepEqual(await c.listConflicts(), []);
      assert.equal((await c.status()).pendingOutbox, '0');
      assert.equal((await c.attachmentStatus(hash)).state, 'Missing');
      await c.downloadAttachment(hash);
      assert.equal((await c.attachmentStatus(hash)).state, 'Available');
      await c.exportAttachment(hash, join(root, 'output'));
      assert.deepEqual(await readFile(join(root, 'output')), bytes);
      // SharedBootstrap includes only published reference history, not arbitrary downloaded cache.
      const shared = await c.backup(join(root, 'shared'), 'SharedBootstrap');
      assert.deepEqual(
        shared.files.map((f) => f.path).sort(),
        [`objects/${hash}`, 'store.db'].sort(),
      );
      assert.deepEqual(await readFile(join(root, 'shared', 'objects', hash)), bytes);
      console.log(
        'C1 fresh third peer: two causal packets; edited row; historical reference in shared manifest; exact downloaded bytes',
      );
      await b.downloadAttachment(hash);
      assert.equal((await b.attachmentStatus(hash)).state, 'Available');
      assert.equal(
        (await b.writeTransaction(edit('explicit after download'), [hash])).committed,
        true,
      );
      assert.deepEqual(await b.read('SELECT * FROM items'), [
        [text('one'), text('explicit after download'), text(hash)],
      ]);
      assert.equal((await b.status()).pendingOutbox, '1');
      console.log('C1 Available explicit reference after download: committed');
    } finally {
      await Promise.all([a, b, c].filter(Boolean).map((db) => db.close()));
      await rm(root, { recursive: true, force: true });
    }
  },
);
