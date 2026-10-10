import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, readdir, appendFile } from 'node:fs/promises';
import { join, relative } from 'node:path';
import { createHash } from 'node:crypto';
import { open as publicOpen, restore as publicRestore, value as v } from '../index.js';
const root = process.env.NH1_FIXTURE,
  evidence = process.env.NH1_EVIDENCE;
async function open(options) {
  await appendFile(
    join(evidence, 'public-api.jsonl'),
    JSON.stringify({ operation: 'open', configuration: '[REDACTED]' }) + '\n',
  );
  return publicOpen(options);
}
async function restore(options) {
  await appendFile(
    join(evidence, 'public-api.jsonl'),
    JSON.stringify({ operation: 'restore', configuration: '[REDACTED]' }) + '\n',
  );
  return publicRestore(options);
}
const backend = {
  kind: 'Webdav',
  endpoint: process.env.NH1_ENDPOINT,
  credentials: {
    kind: 'Basic',
    username: process.env.NH1_USER,
    password: process.env.NH1_PASSWORD,
  },
  allow_loopback_http: true,
};
const schema = {
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
const opts = (device, dataset = 'nh1-journey') => ({
  path: join(root, `${dataset}-${device}.db`),
  identity: [dataset, device, 'first'],
  schema,
  backend,
});
const row = (id, t = null) => [
  v.text(id),
  v.integer(9223372036854775807n),
  v.real(1.125),
  v.blob(Buffer.from([0, 255, 128])),
  t === null ? v.null() : v.text(t),
];
const put = (id, t) => ({ sql: 'INSERT INTO items VALUES (?,?,?,?,?)', params: row(id, t) });
const update = (t) => ({
  sql: 'UPDATE items SET t=? WHERE id=?',
  params: [v.text(t), v.text('one')],
});
const sha = (bytes) => createHash('sha256').update(bytes).digest('hex');
async function files(path) {
  const result = [];
  for (const e of await readdir(path, { withFileTypes: true })) {
    const p = join(path, e.name);
    if (e.isDirectory()) result.push(...(await files(p)));
    else result.push(p);
  }
  return result.sort();
}
async function artifact(name, data) {
  await writeFile(join(evidence, `${name}.json`), JSON.stringify(data, null, 2));
}
async function remoteObjects() {
  const result = {};
  for (const path of await files(join(root, 'remote')))
    result[relative(join(root, 'remote'), path)] = sha(await readFile(path));
  return result;
}
async function wires() {
  const result = {};
  for (const p of await files(join(root, 'remote'))) {
    if (!p.endsWith('.lfs')) continue;
    const bytes = await readFile(p),
      key = relative(join(root, 'remote'), p);
    assert.equal(p.slice(-68, -4), sha(bytes));
    const response = await fetch(`${backend.endpoint}/${key}`, {
      headers: {
        Authorization:
          'Basic ' +
          Buffer.from(`${backend.credentials.username}:${backend.credentials.password}`).toString(
            'base64',
          ),
      },
    });
    assert.equal(response.status, 200);
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), bytes);
    result[key] = { sha256: sha(bytes), bytes: bytes.toString('base64') };
  }
  return result;
}
async function nativeProof() {
  const path = join(process.cwd(), 'bindings/nodejs/lfs_node.node');
  const bytes = await readFile(path);
  assert.deepEqual([...bytes.subarray(0, 4)], [127, 69, 76, 70]);
  const loaded = process.report.getReport().sharedObjects.filter((p) => p === path);
  assert.deepEqual(loaded, [path]);
  await artifact('node-native', { node: process.version, loaded, sha256: sha(bytes) });
}
test('H1 public Basic typed Session and three-device convergence', { timeout: 60000 }, async () => {
  await nativeProof();
  const handles = [];
  try {
    const a = await open(opts('a'));
    handles.push(a);
    const b = await open(opts('b'));
    handles.push(b);
    const c = await open(opts('c'));
    handles.push(c);
    await a.writeTransaction([put('one'), put('typed', '跨设备')]);
    assert.equal((await a.syncOnce()).healthy, true);
    await b.syncOnce();
    await c.syncOnce();
    assert.deepEqual(await b.read('SELECT * FROM items ORDER BY id'), [
      row('one'),
      row('typed', '跨设备'),
    ]);
    assert.equal((await b.status()).pendingOutbox, '0');
    await a.writeTransaction([update('left')]);
    await b.writeTransaction([update('right')]);
    await c.writeTransaction([{ sql: 'DELETE FROM items WHERE id=?', params: [v.text('one')] }]);
    await a.syncOnce();
    await b.syncOnce();
    await c.syncOnce();
    await a.syncOnce();
    await b.syncOnce();
    const heads = await a.listConflicts();
    assert.equal(heads.length, 1);
    assert.equal(heads[0].heads.length, 3);
    assert.deepEqual(await b.listConflicts(), heads);
    assert.deepEqual(await c.listConflicts(), heads);
    assert.ok(heads[0].heads.some((h) => h.row === null));
    assert.deepEqual(
      heads[0].heads
        .filter((h) => h.row)
        .map((h) => h.row[4].value)
        .sort(),
      ['left', 'right'],
    );
    await assert.rejects(
      a.writeTransaction([
        { sql: 'INSERT OR REPLACE INTO items VALUES (?,?,?,?,?)', params: row('one', 'implicit') },
      ]),
      { code: 'ConflictRequiresResolution' },
    );
    await a.resolve({
      table: 1,
      key: 'one',
      observed: heads[0].heads.map((h) => h.id),
      row: row('one', 'resolved'),
    });
    await a.syncOnce();
    await b.syncOnce();
    await c.syncOnce();
    const final = [];
    for (const db of handles) {
      assert.deepEqual(await db.listConflicts(), []);
      assert.deepEqual(await db.read('SELECT * FROM items ORDER BY id'), [
        row('one', 'resolved'),
        row('typed', '跨设备'),
      ]);
      const status = await db.status();
      assert.equal(status.pendingOutbox, '0');
      assert.equal(status.healthy, true);
      final.push(status);
    }
    const remote = await wires();
    assert.equal(Object.keys(remote).length, 5);
    await artifact('H1', { heads, final, remote });
  } finally {
    for (const db of handles) await db.close();
  }
});

test('H2 public Bearer MODEL_HTTP performs real record exchange', { timeout: 60000 }, async () => {
  const model = {
    ...backend,
    endpoint: process.env.NH1_BEARER_ENDPOINT,
    credentials: { kind: 'Bearer', token: process.env.NH1_BEARER },
  };
  const handles = [];
  try {
    const options = { ...opts('a', 'nh1-bearer'), backend: model };
    const wrong = await open({
      ...options,
      backend: { ...model, credentials: { kind: 'Bearer', token: 'nh1-wrong-synthetic-bearer' } },
    });
    try {
      await wrong.writeTransaction([put('bearer', '真实 HTTP')]);
      await assert.rejects(wrong.syncOnce(), { code: 'Auth' });
      assert.equal((await wrong.status()).pendingOutbox, '1');
      await noSecrets([], [await wrong.info(), await wrong.events()]);
      await dbNoSecrets(options.path);
    } finally {
      await wrong.close();
    }
    const a = await open(options);
    handles.push(a);
    const b = await open({ ...opts('b', 'nh1-bearer'), backend: model });
    handles.push(b);
    assert.equal((await a.status()).pendingOutbox, '1');
    assert.equal((await a.syncOnce()).healthy, true);
    assert.equal((await b.syncOnce()).healthy, true);
    assert.deepEqual(await b.read('SELECT * FROM items'), [row('bearer', '真实 HTTP')]);
    assert.equal((await b.status()).pendingOutbox, '0');
    await artifact('H2', {
      category: 'MODEL_HTTP',
      remote: await wires(),
      status: await b.status(),
    });
  } finally {
    for (const db of handles) await db.close();
  }
});

test('H3 characterization: strict configuration fails before SQLite open or restore', async () => {
  const invalid = [];
  const basic = backend.credentials;
  const bad = (label, patch, code = 'Invalid') =>
    invalid.push({ label, config: { ...backend, ...patch }, code });
  bad('empty endpoint', { endpoint: '' });
  bad('long endpoint', { endpoint: 'x'.repeat(8193) });
  bad('extra field', { unexpected: 'hidden' });
  bad('null flag', { allow_loopback_http: null });
  bad('string flag', { allow_loopback_http: 'true' });
  bad('numeric flag', { allow_loopback_http: 1 });
  for (const field of ['username', 'password']) {
    bad(`empty ${field}`, { credentials: { ...basic, [field]: '' } });
    bad(`long ${field}`, { credentials: { ...basic, [field]: '密'.repeat(2731) } });
  }
  bad('colon username', { credentials: { ...basic, username: 'user:id' } });
  bad('unknown credential', { credentials: { kind: 'OAuth', token: 'hidden' } });
  bad('extra credential', { credentials: { ...basic, token: 'hidden' } });
  for (const token of [
    '',
    'a b',
    'a\tb',
    'a\nb',
    'a\rb',
    'a\u0000b',
    '密',
    'abc!',
    'a=b',
    '=',
    'a'.repeat(8193),
  ])
    bad(`bad token ${invalid.length}`, { credentials: { kind: 'Bearer', token } });
  bad('bearer extra', { credentials: { kind: 'Bearer', token: 'abc', username: 'hidden' } });
  bad('default insecure', { allow_loopback_http: undefined }, 'Unsupported');
  bad('false insecure', { allow_loopback_http: false }, 'Unsupported');
  for (const endpoint of [
    'http://example.invalid',
    'http://localhost',
    'ftp://127.0.0.1',
    'https://user:pass@example.invalid',
    'https://example.invalid/?hidden=token',
    'https://example.invalid/#hidden',
    'not-a-url',
  ])
    bad(`unsafe endpoint ${invalid.length}`, { endpoint }, 'Unsupported');
  invalid.push(
    { label: 'S3 not exported', config: { kind: 'S3' }, code: 'Invalid' },
    { label: 'Drive deferred', config: { kind: 'GoogleDrive' }, code: 'Invalid' },
  );
  const results = [];
  for (const [i, item] of invalid.entries()) {
    const options = { ...opts(`invalid-${i}`, 'nh1-config'), backend: item.config };
    await assert.rejects(open(options), { code: item.code });
    await assert.rejects(readFile(options.path), { code: 'ENOENT' });
    const destination = join(root, `invalid-restore-${i}`);
    await assert.rejects(
      restore({ backup: join(root, 'no-backup'), destination, backend: item.config }),
      { code: item.code },
    );
    await assert.rejects(readdir(destination), { code: 'ENOENT' });
    results.push({
      label: item.label,
      code: item.code,
      noDatabase: true,
      noRestoreDestination: true,
    });
  }
  for (const [i, credentials] of [
    { kind: 'Basic', username: 'u'.repeat(8192), password: '密:pass' },
    { kind: 'Basic', username: 'user', password: 'p'.repeat(8192) },
    { kind: 'Bearer', token: 'a'.repeat(8192) },
    { kind: 'Bearer', token: 'AZaz09-._~+/==' },
  ].entries()) {
    const db = await open({
      ...opts(`valid-${i}`, 'nh1-config'),
      backend: { ...backend, endpoint: 'https://example.invalid', credentials },
    });
    await db.close();
  }
  await artifact('H3', { cases: results, acceptedBoundaryCases: 4 });
});

async function replayBackup(backup, label) {
  const { mkdir } = await import('node:fs/promises');
  const remote = join(root, `${label}-opaque-replay`);
  await mkdir(remote);
  const db = await restore({
    backup,
    destination: join(root, `${label}-restored-fs`),
    backend: { kind: 'Fs', root: remote },
  });
  try {
    assert.equal((await db.syncOnce()).healthy, true);
    const result = {};
    for (const path of await files(remote))
      if (path.endsWith('.lfs')) {
        const bytes = await readFile(path);
        assert.equal(path.slice(-68, -4), sha(bytes));
        result[relative(remote, path)] = { sha256: sha(bytes), bytes: bytes.toString('base64') };
      }
    return result;
  } finally {
    await db.close();
  }
}
async function noSecrets(paths, outputs = []) {
  const secrets = [
    backend.credentials.username,
    backend.credentials.password,
    process.env.NH1_BEARER,
    'nh1-wrong-synthetic-password',
    'nh1-wrong-synthetic-bearer',
  ].filter(Boolean);
  for (const path of paths)
    for (const file of await files(path)) {
      const bytes = await readFile(file);
      for (const secret of secrets)
        assert.equal(bytes.includes(Buffer.from(secret)), false, `secret in SDK file ${file}`);
    }
  for (const output of outputs)
    for (const secret of secrets) assert.equal(JSON.stringify(output).includes(secret), false);
}
async function dbNoSecrets(path) {
  const { dirname, basename } = await import('node:path');
  const candidates = (await readdir(dirname(path))).filter(
    (p) => p === basename(path) || p.startsWith(basename(path) + '-'),
  );
  for (const p of candidates) {
    const bytes = await readFile(join(dirname(path), p));
    for (const secret of [
      backend.credentials.username,
      backend.credentials.password,
      process.env.NH1_BEARER,
      'nh1-wrong-synthetic-password',
      'nh1-wrong-synthetic-bearer',
    ].filter(Boolean))
      assert.equal(bytes.includes(Buffer.from(secret)), false);
  }
}
test(
  'H4 characterization: expired Basic snapshot, exact pending retry, local restore rotation and redaction',
  { timeout: 60000 },
  async () => {
    const options = opts('source', 'nh1-recovery');
    let db, copy;
    try {
      db = await open(options);
      await db.writeTransaction([put('pending', 'survives expiration')]);
      const before = await db.status(),
        identity = (await db.info()).identity;
      const backup = join(root, 'recovery-before');
      await db.backup(backup, 'LocalDisaster');
      const expected = await replayBackup(backup, 'recovery-before');
      assert.equal(Object.keys(expected).length, 1);
      await db.close();
      db = null;
      const wrongBackend = {
        ...backend,
        credentials: { ...backend.credentials, password: 'nh1-wrong-synthetic-password' },
      };
      db = await open({ ...options, backend: wrongBackend });
      wrongBackend.credentials.password = backend.credentials.password;
      let error;
      try {
        await db.syncOnce();
        assert.fail('expected Auth');
      } catch (e) {
        assert.equal(e.code, 'Auth');
        error = { name: e.name, message: e.message, code: e.code };
      }
      assert.deepEqual(await db.status(), { ...before, healthy: false });
      assert.deepEqual((await db.info()).identity, identity);
      const after = join(root, 'recovery-after-auth');
      await db.backup(after, 'LocalDisaster');
      assert.deepEqual(await replayBackup(after, 'recovery-after'), expected);
      const diagnostics = [await db.info(), await db.events(), error];
      assert.ok(diagnostics[1].some((e) => e.operation === 'sync' && e.error === 'Auth'));
      await dbNoSecrets(options.path);
      await noSecrets([backup, after], diagnostics);
      await db.close();
      db = null;
      db = await open(options);
      assert.deepEqual(await db.status(), before);
      assert.deepEqual((await db.info()).identity, identity);
      assert.equal((await db.syncOnce()).healthy, true);
      const remote = await wires();
      for (const [path, bytes] of Object.entries(expected)) assert.deepEqual(remote[path], bytes);
      copy = await restore({ backup: after, destination: join(root, 'recovery-webdav'), backend });
      assert.equal((await copy.status()).pendingOutbox, '1');
      const rotated = (await copy.info()).identity;
      assert.deepEqual(rotated.slice(0, 2), identity.slice(0, 2));
      assert.notEqual(rotated[2], identity[2]);
      await copy.writeTransaction([put('new-incarnation', 'new operation')]);
      assert.equal((await copy.syncOnce()).healthy, true);
      await db.syncOnce();
      assert.deepEqual(await db.read('SELECT * FROM items ORDER BY id'), [
        row('new-incarnation', 'new operation'),
        row('pending', 'survives expiration'),
      ]);
      const finalRemote = await wires();
      for (const [path, bytes] of Object.entries(expected))
        assert.deepEqual(finalRemote[path], bytes);
      assert.ok(
        Object.keys(finalRemote).some((p) =>
          p.includes('/' + Buffer.from(rotated[2]).toString('base64url') + '/'),
        ),
      );
      await artifact('H4', {
        before,
        identity,
        rotated,
        diagnostics,
        expected,
        final: await db.status(),
        copyFinal: await copy.status(),
        remote: finalRemote,
      });
    } finally {
      if (db) await db.close();
      if (copy) await copy.close();
    }
  },
);

test(
  'H5 characterization: WebDAV referenced attachment refuses before remote record and preserves exact pending',
  { timeout: 60000 },
  async () => {
    const options = opts('source', 'nh1-attachment');
    let db, copy;
    try {
      db = await open(options);
      const input = join(root, 'synthetic-attachment');
      await writeFile(input, Buffer.from('NH1 synthetic attachment\0bytes'));
      const attachment = await db.addAttachment(input);
      await db.writeTransaction([put('one', attachment.hash)], [attachment.hash]);
      const before = await db.status(),
        identity = (await db.info()).identity,
        remoteBefore = await remoteObjects();
      const backup = join(root, 'attachment-before');
      await db.backup(backup, 'LocalDisaster');
      const expected = await replayBackup(backup, 'attachment-before');
      assert.equal(Object.keys(expected).length, 1);
      await assert.rejects(db.syncOnce(), { code: 'Unsupported' });
      assert.deepEqual(await db.status(), { ...before, healthy: false });
      assert.deepEqual((await db.info()).identity, identity);
      assert.deepEqual(await remoteObjects(), remoteBefore);
      await assert.rejects(db.downloadAttachment(attachment.hash), { code: 'Unsupported' });
      assert.deepEqual(await remoteObjects(), remoteBefore);
      const after = join(root, 'attachment-after');
      await db.backup(after, 'LocalDisaster');
      assert.deepEqual(await replayBackup(after, 'attachment-after'), expected);
      await db.close();
      db = null;
      db = await open(options);
      assert.deepEqual(await db.status(), before);
      copy = await restore({
        backup: after,
        destination: join(root, 'attachment-webdav'),
        backend,
      });
      const rotated = (await copy.info()).identity;
      assert.notEqual(rotated[2], identity[2]);
      assert.equal((await copy.status()).pendingOutbox, '1');
      await assert.rejects(copy.syncOnce(), { code: 'Unsupported' });
      assert.deepEqual(await remoteObjects(), remoteBefore);
      const restoredBackup = join(root, 'attachment-restored-backup');
      await copy.backup(restoredBackup, 'LocalDisaster');
      assert.deepEqual(await replayBackup(restoredBackup, 'attachment-restored'), expected);
      await noSecrets(
        [backup, after, restoredBackup],
        [await db.info(), await db.events(), await copy.info()],
      );
      await dbNoSecrets(options.path);
      await artifact('H5', {
        before,
        identity,
        rotated,
        attachment,
        expected,
        remoteUnchanged: true,
        final: await db.status(),
        restored: await copy.status(),
      });
    } finally {
      if (db) await db.close();
      if (copy) await copy.close();
    }
  },
);

test(
  'H6 characterization: active HTTP abort, Busy, close drain and preabort preserve pending',
  { timeout: 60000 },
  async () => {
    const { unlink } = await import('node:fs/promises');
    const model = {
      ...backend,
      endpoint: process.env.NH1_BEARER_ENDPOINT,
      credentials: { kind: 'Bearer', token: process.env.NH1_BEARER },
    };
    const options = { ...opts('source', 'nh1-cancel'), backend: model };
    let db;
    try {
      db = await open(options);
      await db.writeTransaction([put('pending', 'cancel and retry')]);
      const before = await db.status();
      const backup = join(root, 'cancel-before');
      await db.backup(backup, 'LocalDisaster');
      const expected = await replayBackup(backup, 'cancel-before');
      const pre = new AbortController();
      pre.abort();
      await assert.rejects(db.syncOnce({ signal: pre.signal }), { code: 'Cancelled' });
      assert.deepEqual(await db.status(), before);
      const preabortEvents = await db.events();
      assert.ok(preabortEvents.some((e) => e.error === 'Cancelled'));
      const tracePath = join(evidence, 'model-http.jsonl');
      const count = (await readFile(tracePath, 'utf8')).trim().split('\n').length;
      await writeFile(join(root, 'delay-http'), 'synthetic bounded 2 second delay');
      const controller = new AbortController();
      const sync = db.syncOnce({ signal: controller.signal });
      const rejection = assert.rejects(sync, { code: 'Cancelled' });
      await assert.rejects(db.syncOnce(), { code: 'Busy' });
      let received = false;
      for (let i = 0; i < 100; i++) {
        const events = (await readFile(tracePath, 'utf8'))
          .trim()
          .split('\n')
          .slice(count)
          .map(JSON.parse);
        if (events.some((e) => e.phase === 'received' && e.authorized)) {
          received = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 10));
      }
      assert.equal(received, true, 'cancel must intersect an actual HTTP request');
      const closed = db.close();
      controller.abort();
      await rejection;
      await closed;
      db = null;
      await unlink(join(root, 'delay-http'));
      db = await open(options);
      assert.deepEqual(await db.status(), before);
      const after = join(root, 'cancel-after');
      await db.backup(after, 'LocalDisaster');
      assert.deepEqual(await replayBackup(after, 'cancel-after'), expected);
      assert.equal((await db.syncOnce()).healthy, true);
      const remote = await wires();
      for (const [path, bytes] of Object.entries(expected)) assert.deepEqual(remote[path], bytes);
      await noSecrets([backup, after], [await db.info(), await db.events()]);
      await dbNoSecrets(options.path);
      await artifact('H6', {
        before,
        preabortEvents,
        actualHttpBeforeAbort: received,
        expected,
        final: await db.status(),
        events: await db.events(),
      });
    } finally {
      if (db) await db.close();
    }
  },
);
