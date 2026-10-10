import test from 'node:test';
import assert from 'node:assert/strict';
import {
  readFile,
  writeFile,
  readdir,
  appendFile,
  mkdir,
  open as fileOpen,
  unlink,
} from 'node:fs/promises';
import { join, relative } from 'node:path';
import { createHash, createHmac } from 'node:crypto';
import { open as sdkOpen, restore, value as v } from '../index.js';
const root = process.env.NH2_FIXTURE,
  evidence = process.env.NH2_EVIDENCE;
const backend = {
  kind: 'S3',
  endpoint: process.env.NH2_ENDPOINT,
  region: 'us-east-1',
  bucket: process.env.NH2_BUCKET,
  credentials: { access_key_id: process.env.NH2_ACCESS, secret_access_key: process.env.NH2_SECRET },
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
const opts = (device, dataset) => ({
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
const sha = (b) => createHash('sha256').update(b).digest('hex');
async function artifact(name, data) {
  await writeFile(join(evidence, name + '.json'), JSON.stringify(data, null, 2));
}
async function open(options) {
  await appendFile(
    join(evidence, 'public-api.jsonl'),
    JSON.stringify({
      operation: 'open',
      dataset: options.identity[0],
      configuration: '[REDACTED]',
    }) + '\n',
  );
  try {
    return await sdkOpen(options);
  } catch (e) {
    await appendFile(
      join(evidence, 'public-api.jsonl'),
      JSON.stringify({ operation: 'open', code: e.code, message: e.message }) + '\n',
    );
    throw e;
  }
}
async function files(path) {
  const out = [];
  for (const e of await readdir(path, { withFileTypes: true })) {
    const p = join(path, e.name);
    if (e.isDirectory()) out.push(...(await files(p)));
    else out.push(p);
  }
  return out.sort();
}
async function nativeProof() {
  const path = join(process.cwd(), 'bindings/nodejs/lfs_node.node'),
    bytes = await readFile(path);
  assert.deepEqual([...bytes.subarray(0, 4)], [127, 69, 76, 70]);
  const loaded = process.report.getReport().sharedObjects.filter((p) => p === path);
  assert.deepEqual(loaded, [path]);
  await artifact('node-native', {
    path,
    loaded,
    sha256: sha(bytes),
    node: process.version,
    pid: process.pid,
    ppid: process.ppid,
  });
}
if (evidence) await nativeProof();
async function httpGet(key) {
  const path = '/' + backend.bucket + '/' + key,
    stamp = new Date().toISOString().replace(/[:-]|\.\d{3}/g, ''),
    day = stamp.slice(0, 8),
    host = new URL(backend.endpoint).host;
  const empty = sha(''),
    signed = 'host;x-amz-content-sha256;x-amz-date',
    headers = `host:${host}\nx-amz-content-sha256:${empty}\nx-amz-date:${stamp}\n`;
  const canonical = `GET\n${path}\n\n${headers}\n${signed}\n${empty}`,
    scope = `${day}/${backend.region}/s3/aws4_request`;
  const message = `AWS4-HMAC-SHA256\n${stamp}\n${scope}\n${sha(canonical)}`;
  let k = Buffer.from('AWS4' + backend.credentials.secret_access_key);
  for (const s of [day, backend.region, 's3', 'aws4_request'])
    k = createHmac('sha256', k).update(s).digest();
  const signature = createHmac('sha256', k).update(message).digest('hex');
  const response = await fetch(backend.endpoint + path, {
    headers: {
      'x-amz-date': stamp,
      'x-amz-content-sha256': empty,
      Authorization: `AWS4-HMAC-SHA256 Credential=${backend.credentials.access_key_id}/${scope}, SignedHeaders=${signed}, Signature=${signature}`,
    },
  });
  assert.equal(response.status, 200);
  return Buffer.from(await response.arrayBuffer());
}
async function wires(dataset) {
  const result = {},
    remote = join(root, 'remote', backend.bucket),
    prefix = 'lfs1/' + Buffer.from(dataset).toString('base64url') + '/';
  for (const p of await files(remote)) {
    const key = relative(remote, p);
    if (!key.startsWith(prefix) || !key.endsWith('.lfs')) continue;
    const b = await httpGet(key);
    assert.deepEqual(b, await readFile(p));
    assert.equal(key.slice(-68, -4), sha(b));
    result[key] = { sha256: sha(b), bytes: b.toString('base64') };
  }
  return result;
}

test(
  'S1 public S3 records typed full rows conflict resolution delete noecho reopen',
  { timeout: 60000 },
  async () => {
    await nativeProof();
    const dataset = 'nh2-records',
      handles = [];
    try {
      const a = await open(opts('a', dataset));
      handles.push(a);
      const b = await open(opts('b', dataset));
      handles.push(b);
      const c = await open(opts('c', dataset));
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
      await assert.rejects(
        a.writeTransaction([
          {
            sql: 'INSERT OR REPLACE INTO items VALUES (?,?,?,?,?)',
            params: row('one', 'implicit'),
          },
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
      for (const db of handles) {
        assert.deepEqual(await db.listConflicts(), []);
        assert.deepEqual(await db.read('SELECT * FROM items ORDER BY id'), [
          row('one', 'resolved'),
          row('typed', '跨设备'),
        ]);
        assert.equal((await db.status()).pendingOutbox, '0');
      }
      const remote = await wires(dataset);
      assert.equal(Object.keys(remote).length, 5);
      await artifact('S1', {
        heads,
        remote,
        rows: await b.read('SELECT * FROM items ORDER BY id'),
      });
    } finally {
      for (const db of handles) await db.close();
    }
    const reopened = await open(opts('a', dataset));
    try {
      assert.equal((await reopened.status()).pendingOutbox, '0');
      assert.deepEqual(await reopened.read('SELECT * FROM items ORDER BY id'), [
        row('one', 'resolved'),
        row('typed', '跨设备'),
      ]);
    } finally {
      await reopened.close();
    }
  },
);

test(
  'S2 S3 configuration token absent versus null and strict pre-sideeffect validation',
  { timeout: 60000 },
  async () => {
    const invalid = [];
    const bad = (label, patch, code = 'Invalid') =>
      invalid.push({ label, backend: { ...backend, ...patch }, code });
    // First assertion isolates serde Option null acceptance from all other policy checks.
    bad('present null token', { credentials: { ...backend.credentials, session_token: null } });
    for (const field of ['endpoint', 'region', 'bucket'])
      for (const value of ['', null, 17, '密'.repeat(2731)])
        bad(`${field} invalid ${invalid.length}`, { [field]: value });
    for (const field of ['access_key_id', 'secret_access_key', 'session_token'])
      for (const value of ['', null, 17, {}, '密'.repeat(2731)])
        bad(`${field} invalid ${invalid.length}`, {
          credentials: { ...backend.credentials, [field]: value },
        });
    bad('unknown backend field', { extra: 'no' });
    bad('unknown credential field', { credentials: { ...backend.credentials, extra: 'no' } });
    bad('credential kind forbidden', { credentials: { ...backend.credentials, kind: 'Static' } });
    for (const value of [null, 'true', 1])
      bad('nonboolean flag ' + invalid.length, { allow_loopback_http: value });
    bad('default HTTPS', { allow_loopback_http: undefined }, 'Unsupported');
    bad('false HTTP', { allow_loopback_http: false }, 'Unsupported');
    for (const endpoint of [
      'http://localhost',
      'http://127.0.0.2',
      'http://example.invalid',
      'ftp://127.0.0.1',
      'https://user:pass@example.invalid',
      'https://example.invalid/?secret=no',
      'https://example.invalid/#secret',
      'bad-url',
    ])
      bad('unsafe URL ' + invalid.length, { endpoint }, 'Unsupported');
    const results = [];
    async function mustReject(operation, code) {
      let handle;
      try {
        await assert.rejects(
          async () => {
            handle = await operation();
          },
          { code },
        );
      } finally {
        if (handle) await handle.close();
      }
    }
    for (const [i, c] of invalid.entries()) {
      const options = { ...opts('bad-' + i, 'nh2-config'), backend: c.backend };
      await mustReject(() => open(options), c.code);
      await assert.rejects(readFile(options.path), { code: 'ENOENT' });
      const destination = join(root, 'bad-restore-' + i);
      await mustReject(
        () => restore({ backup: join(root, 'absent-backup'), destination, backend: c.backend }),
        c.code,
      );
      await assert.rejects(readdir(destination), { code: 'ENOENT' });
      results.push({ label: c.label, code: c.code, noDB: true, noRestoreDestination: true });
    }
    let accepted = 0;
    for (const field of [
      'endpoint',
      'region',
      'bucket',
      'access_key_id',
      'secret_access_key',
      'session_token',
    ]) {
      const b = structuredClone(backend);
      b.endpoint = 'https://example.invalid';
      const value =
        field === 'endpoint'
          ? 'https://example.invalid/' + 'a'.repeat(8192 - 24)
          : 'a'.repeat(8192);
      assert.equal(Buffer.byteLength(value), 8192);
      if (field in b) b[field] = value;
      else b.credentials[field] = value;
      const db = await open({ ...opts('boundary-' + field, 'nh2-config'), backend: b });
      await db.close();
      accepted++;
    }
    const db = await open(opts('cancel', 'nh2-config'));
    try {
      await db.writeTransaction([put('cancel')]);
      const before = await db.status();
      const ctrl = new AbortController();
      ctrl.abort();
      await assert.rejects(db.syncOnce({ signal: ctrl.signal }), { code: 'Cancelled' });
      assert.deepEqual(await db.status(), before);
      const backup = join(root, 'cancel-backup');
      await db.backup(backup, 'LocalDisaster');
      const destination = join(root, 'cancel-restore');
      await mustReject(
        () => restore({ backup, destination, backend }, { signal: ctrl.signal }),
        'Cancelled',
      );
      await assert.rejects(readdir(destination), { code: 'ENOENT' });
    } finally {
      await db.close();
    }
    await artifact('S2', {
      results,
      accepted8192ByteBoundaries: accepted,
      cancelledBeforeSideeffects: true,
    });
  },
);

async function replayBackup(backup, label) {
  const remote = join(root, label + '-fs-oracle');
  await mkdir(remote);
  const db = await restore({
    backup,
    destination: join(root, label + '-restored-fs'),
    backend: { kind: 'Fs', root: remote },
  });
  try {
    assert.equal((await db.syncOnce()).healthy, true);
    const out = {};
    for (const p of await files(remote))
      if (p.endsWith('.lfs')) {
        const b = await readFile(p);
        assert.equal(p.slice(-68, -4), sha(b));
        out[relative(remote, p)] = { sha256: sha(b), bytes: b.toString('base64') };
      }
    return out;
  } finally {
    await db.close();
  }
}
async function noSecrets(paths, outputs = []) {
  const secrets = [
    backend.credentials.access_key_id,
    backend.credentials.secret_access_key,
    process.env.NH2_TOKEN,
    'NH2_WRONG_SYNTHETIC_ACCESS',
  ];
  for (const path of paths)
    for (const p of await files(path)) {
      const b = await readFile(p);
      for (const s of secrets)
        assert.equal(b.includes(Buffer.from(s)), false, 'credential persisted in SDK output');
    }
  for (const o of outputs)
    for (const s of secrets)
      assert.equal(JSON.stringify(o).includes(s), false, 'credential exposed by SDK diagnostics');
}
async function dbNoSecrets(path) {
  const secrets = [
    backend.credentials.access_key_id,
    backend.credentials.secret_access_key,
    process.env.NH2_TOKEN,
    'NH2_WRONG_SYNTHETIC_ACCESS',
  ];
  for (const p of await files(root))
    if (p === path || p.startsWith(path + '-')) {
      const b = await readFile(p);
      for (const s of secrets)
        assert.equal(b.includes(Buffer.from(s)), false, 'credential persisted in database');
    }
}
async function trace() {
  return (await readFile(join(evidence, 'http.jsonl'), 'utf8'))
    .trim()
    .split('\n')
    .filter(Boolean)
    .map(JSON.parse);
}
const datasetPrefix = (dataset) => 'lfs1/' + Buffer.from(dataset).toString('base64url') + '/';

test(
  'S3 characterization auth snapshot exact pending recovery S3 restore and token forwarding',
  { timeout: 60000 },
  async () => {
    const dataset = 'nh2-auth',
      options = opts('source', dataset);
    let db, copy, peer;
    const backups = [],
      errors = [];
    try {
      const wrong = {
        ...backend,
        credentials: { ...backend.credentials, access_key_id: 'NH2_WRONG_SYNTHETIC_ACCESS' },
      };
      db = await open({ ...options, backend: wrong });
      await db.writeTransaction([put('one', 'original pending bytes')]);
      const before = await db.status(),
        identity = (await db.info()).identity;
      const initial = join(root, 'auth-initial');
      backups.push(initial);
      await db.backup(initial, 'LocalDisaster');
      const expected = await replayBackup(initial, 'auth-initial');
      assert.equal(Object.keys(expected).length, 1);
      const authFail = async () => {
        try {
          await db.syncOnce();
          assert.fail('expected real Auth');
        } catch (e) {
          assert.equal(e.code, 'Auth');
          errors.push({ code: e.code, name: e.name, message: e.message });
        }
      };
      await authFail();
      wrong.credentials.access_key_id = backend.credentials.access_key_id;
      await authFail();
      assert.deepEqual(await db.status(), { ...before, healthy: false });
      assert.deepEqual((await db.info()).identity, identity);
      assert.deepEqual(await wires(dataset), {});
      const after = join(root, 'auth-after');
      backups.push(after);
      await db.backup(after, 'LocalDisaster');
      assert.deepEqual(await replayBackup(after, 'auth-after'), expected);
      await noSecrets(backups, [await db.info(), await db.events(), ...errors]);
      await dbNoSecrets(options.path);
      await db.close();
      db = null;
      db = await open(options);
      assert.deepEqual((await db.info()).identity, identity);
      assert.deepEqual(await db.status(), before);
      assert.equal((await db.syncOnce()).healthy, true);
      assert.deepEqual(await wires(dataset), expected);
      copy = await restore({ backup: after, destination: join(root, 'auth-s3-restore'), backend });
      const rotated = (await copy.info()).identity;
      assert.deepEqual(rotated.slice(0, 2), identity.slice(0, 2));
      assert.notEqual(rotated[2], identity[2]);
      assert.equal((await copy.status()).pendingOutbox, '1');
      const restoredBackup = join(root, 'auth-restored');
      backups.push(restoredBackup);
      await copy.backup(restoredBackup, 'LocalDisaster');
      assert.deepEqual(await replayBackup(restoredBackup, 'auth-restored'), expected);
      assert.equal((await copy.syncOnce()).healthy, true);
      await copy.writeTransaction([put('restored', 'new incarnation')]);
      assert.equal((await copy.syncOnce()).healthy, true);
      const published = await wires(dataset);
      for (const [k, b] of Object.entries(expected)) assert.deepEqual(published[k], b);
      assert.equal(Object.keys(published).length, 2);
      assert.ok(
        Object.keys(published).some((k) =>
          k.includes('/' + Buffer.from(rotated[2]).toString('base64url') + '/'),
        ),
      );
      peer = await open(opts('peer', dataset));
      await peer.syncOnce();
      assert.deepEqual(await peer.read('SELECT * FROM items ORDER BY id'), [
        row('one', 'original pending bytes'),
        row('restored', 'new incarnation'),
      ]);
      assert.equal((await peer.status()).pendingOutbox, '0');
      await noSecrets(backups, [await db.info(), await db.events(), await copy.info()]);
      await dbNoSecrets(options.path);
      const requests = (await trace()).filter(
        (e) =>
          e.path.includes(datasetPrefix(dataset)) ||
          e.path.includes(encodeURIComponent(datasetPrefix(dataset))),
      );
      assert.ok(requests.some((e) => e.phase === 'response' && e.status === 403));
      assert.ok(requests.some((e) => e.phase === 'received' && !e.token_present));
      await artifact('S3-auth', {
        before,
        identity,
        rotated,
        errors,
        expected,
        published,
        secretsAbsent: true,
      });
    } finally {
      if (db) await db.close();
      if (copy) await copy.close();
      if (peer) await peer.close();
    }
    const tokenDataset = 'nh2-token',
      withToken = {
        ...backend,
        credentials: { ...backend.credentials, session_token: process.env.NH2_TOKEN },
      };
    const a = await open({ ...opts('a', tokenDataset), backend: withToken });
    let b;
    try {
      await a.writeTransaction([put('token')]);
      assert.equal((await a.syncOnce()).healthy, true);
      b = await open({ ...opts('b', tokenDataset), backend: withToken });
      await b.syncOnce();
      assert.deepEqual(await b.read('SELECT * FROM items'), [row('token')]);
      const events = (await trace()).filter(
        (e) => e.phase === 'received' && e.path.includes(datasetPrefix(tokenDataset)),
      );
      assert.ok(events.length > 0);
      assert.ok(events.every((e) => e.token_present && e.token_matches_synthetic));
      const backup = join(root, 'token-backup');
      await a.backup(backup, 'LocalDisaster');
      await noSecrets([backup], [await a.info(), await a.events()]);
      await dbNoSecrets(opts('a', tokenDataset).path);
      await artifact('S3-token', {
        requests: events.length,
        headerObserved: true,
        valueAndHashRedacted: true,
        qualification: 'synthetic header forwarding; not AWS STS authorization',
      });
    } finally {
      await a.close();
      if (b) await b.close();
    }
  },
);

const attachmentSize = 8388625;
async function generateAttachment(name) {
  const path = join(root, name),
    file = await fileOpen(path, 'wx'),
    digest = createHash('sha256');
  try {
    for (let offset = 0; offset < attachmentSize;) {
      const b = Buffer.alloc(Math.min(65536, attachmentSize - offset));
      for (let i = 0; i < b.length; i++)
        b[i] = ((offset + i) * 31 + Math.floor((offset + i) / 65536)) % 251;
      await file.write(b);
      digest.update(b);
      offset += b.length;
    }
  } finally {
    await file.close();
  }
  return { path, hash: digest.digest('hex'), size: attachmentSize };
}
const attachmentKey = (dataset, hash) =>
  'lfs-attachments/' + Buffer.from(dataset).toString('base64url') + '/' + hash;
async function multipartProof(dataset, input, events) {
  const path = '/' + backend.bucket + '/' + attachmentKey(dataset, input.hash),
    original = await readFile(input.path);
  const firstRecord = events.findIndex(
    (e) =>
      e.phase === 'received' &&
      e.method === 'PUT' &&
      e.path.includes(datasetPrefix(dataset)) &&
      e.path.endsWith('.lfs'),
  );
  assert.ok(firstRecord >= 0, 'real referenced record publication required');
  const before = events.slice(0, firstRecord),
    uploads = before.filter((e) => e.path.startsWith(path));
  const init = uploads.findIndex(
    (e) =>
      e.phase === 'response' &&
      e.method === 'POST' &&
      e.path.includes('uploads') &&
      e.status === 200,
  );
  assert.ok(init >= 0, 'actual multipart initiate forwarded');
  const parts = uploads.filter(
    (e) => e.phase === 'received' && e.method === 'PUT' && e.path.includes('partNumber='),
  );
  assert.equal(parts.length, 2);
  assert.deepEqual(
    parts.map((e) => e.bytes),
    [5242880, 3145745],
  );
  let offset = 0;
  for (const part of parts) {
    assert.equal(part.sha256, sha(original.subarray(offset, offset + part.bytes)));
    offset += part.bytes;
  }
  assert.equal(offset, input.size);
  const complete = uploads.findIndex(
    (e) =>
      e.phase === 'response' &&
      e.method === 'POST' &&
      e.path.includes('uploadId=') &&
      e.status === 200,
  );
  assert.ok(complete > init, 'actual multipart complete acknowledged');
  const verify = uploads
    .slice(complete + 1)
    .filter((e) => e.phase === 'response' && e.method === 'GET' && [200, 206].includes(e.status));
  assert.ok(verify.length > 0);
  let covered = 0;
  for (const e of verify) {
    assert.equal(e.fault, null);
    const m = /bytes (\d+)-(\d+)\/(\d+)/.exec(e.content_range ?? '');
    const start = m ? Number(m[1]) : 0,
      end = m ? Number(m[2]) + 1 : input.size;
    assert.equal(start, covered);
    assert.equal(e.bytes, end - start);
    assert.equal(e.sha256, sha(original.subarray(start, end)));
    covered = end;
  }
  assert.equal(covered, input.size, 'full body verified over real HTTP before record PUT');
  return {
    initiate: true,
    partBytes: parts.map((e) => e.bytes),
    complete: true,
    verificationRanges: verify.length,
    verifiedBytes: covered,
    fullSHA256: sha(original),
    firstRecordTraceIndex: firstRecord,
  };
}

test(
  'S4 characterization actual multipart full-body verification peer Missing download and shared closure',
  { timeout: 90000 },
  async () => {
    const dataset = 'nh2-multipart',
      input = await generateAttachment('multipart-input'),
      handles = [];
    try {
      const a = await open(opts('a', dataset));
      handles.push(a);
      const b = await open(opts('b', dataset));
      handles.push(b);
      const attachment = await a.addAttachment(input.path);
      assert.deepEqual(attachment, {
        hash: input.hash,
        size: String(input.size),
        state: 'Available',
      });
      await a.writeTransaction([put('one', input.hash)], [input.hash]);
      assert.equal((await a.status()).pendingOutbox, '1');
      assert.equal((await a.syncOnce()).healthy, true);
      const proof = await multipartProof(dataset, input, await trace());
      const remote = await httpGet(attachmentKey(dataset, input.hash));
      assert.equal(remote.length, input.size);
      assert.equal(sha(remote), input.hash);
      assert.deepEqual(remote, await readFile(input.path));
      await b.syncOnce();
      assert.deepEqual(await b.read('SELECT * FROM items'), [row('one', input.hash)]);
      assert.equal((await b.status()).pendingOutbox, '0');
      assert.equal((await b.attachmentStatus(input.hash)).state, 'Missing');
      const missingBefore = await b.status();
      await assert.rejects(b.writeTransaction([put('must-not-commit')], [input.hash]), {
        code: 'Invalid',
      });
      assert.deepEqual(await b.status(), missingBefore);
      assert.deepEqual(await b.read('SELECT * FROM items'), [row('one', input.hash)]);
      await b.writeTransaction([
        { sql: 'UPDATE items SET n=? WHERE id=?', params: [v.integer(7n), v.text('one')] },
      ]);
      assert.equal((await b.status()).pendingOutbox, '1');
      assert.equal((await b.attachmentStatus(input.hash)).state, 'Missing');
      const download = await b.downloadAttachment(input.hash);
      assert.deepEqual(download, attachment);
      const exported = join(root, 'multipart-export');
      await b.exportAttachment(input.hash, exported);
      assert.deepEqual(await readFile(exported), remote);
      await b.syncOnce();
      await a.syncOnce();
      const edited = row('one', input.hash);
      edited[1] = v.integer(7n);
      assert.deepEqual(await a.read('SELECT * FROM items'), [edited]);
      const shared = join(root, 'multipart-shared');
      const manifest = await a.backup(shared, 'SharedBootstrap');
      assert.equal(manifest.kind, 'SharedBootstrap');
      assert.ok(
        manifest.files.some((f) => f.sha256 === input.hash && f.size === String(input.size)),
      );
      const clone = await restore({
        backup: shared,
        destination: join(root, 'multipart-clone'),
        backend,
      });
      handles.push(clone);
      assert.notEqual((await clone.info()).identity[2], (await a.info()).identity[2]);
      assert.equal((await clone.status()).pendingOutbox, '0');
      assert.equal((await clone.attachmentStatus(input.hash)).state, 'Available');
      assert.deepEqual(await clone.read('SELECT * FROM items'), [edited]);
      const cloneExport = join(root, 'multipart-clone-export');
      await clone.exportAttachment(input.hash, cloneExport);
      assert.deepEqual(await readFile(cloneExport), remote);
      assert.equal((await clone.syncOnce()).healthy, true);
      await noSecrets([shared], [manifest, await a.info(), await a.events(), await b.events()]);
      await artifact('S4', {
        attachment,
        proof,
        missingExplicitReference: 'Invalid with unchanged persistent state',
        ordinaryHistoricalOfflineEdit: true,
        exportSHA256: sha(await readFile(exported)),
        sharedClosure: manifest,
        cloneIdentity: (await clone.info()).identity,
        remote: await wires(dataset),
      });
    } finally {
      for (const db of handles) await db.close();
    }
  },
);

test(
  'S5 characterization MODEL_HTTP truncated verification retains pending and recovers exact bytes',
  { timeout: 90000 },
  async () => {
    const dataset = 'nh2-fault',
      options = opts('a', dataset),
      input = await generateAttachment('fault-input');
    let a, b;
    const backups = [];
    try {
      a = await open(options);
      b = await open(opts('b', dataset));
      const attachment = await a.addAttachment(input.path);
      await a.writeTransaction([put('one', input.hash)], [input.hash]);
      const before = await a.status(),
        identity = (await a.info()).identity;
      const initial = join(root, 'fault-initial');
      backups.push(initial);
      await a.backup(initial, 'LocalDisaster');
      const expected = await replayBackup(initial, 'fault-initial');
      assert.equal(Object.keys(expected).length, 1);
      await writeFile(
        join(root, 'truncate'),
        'MODEL_HTTP: truncate real successful attachment GET response; do not synthesize S3 messages',
      );
      let failure;
      try {
        await a.syncOnce();
        assert.fail('verification truncation must fail');
      } catch (e) {
        assert.equal(e.code, 'Transient');
        failure = { code: e.code, name: e.name, message: e.message };
      }
      assert.deepEqual(await a.status(), { ...before, healthy: false });
      assert.deepEqual((await a.info()).identity, identity);
      assert.deepEqual(await wires(dataset), {});
      const failedTrace = await trace();
      assert.ok(failedTrace.some((e) => e.fault === 'MODEL_HTTP_TRUNCATE'));
      assert.equal(
        failedTrace.some(
          (e) =>
            e.phase === 'received' &&
            e.method === 'PUT' &&
            e.path.includes(datasetPrefix(dataset)) &&
            e.path.endsWith('.lfs'),
        ),
        false,
      );
      assert.equal((await b.attachmentStatus(input.hash)).state, 'Missing');
      await assert.rejects(b.downloadAttachment(input.hash), { code: 'Transient' });
      assert.equal((await b.attachmentStatus(input.hash)).state, 'Missing');
      assert.deepEqual(await b.read('SELECT * FROM items'), []);
      const failed = join(root, 'fault-after');
      backups.push(failed);
      await a.backup(failed, 'LocalDisaster');
      assert.deepEqual(await replayBackup(failed, 'fault-after'), expected);
      await artifact('S5-failure', {
        category: 'MODEL_HTTP',
        failure,
        before,
        after: await a.status(),
        expected,
        recordPublication: false,
        falseAvailable: false,
        traceEvents: (await trace()).length,
      });
      await noSecrets(backups, [await a.info(), await a.events(), failure]);
      await a.close();
      a = null;
      await b.close();
      b = null;
      await unlink(join(root, 'truncate'));
      a = await open(options);
      b = await open(opts('b', dataset));
      assert.deepEqual(await a.status(), before);
      assert.deepEqual((await a.info()).identity, identity);
      const ctrl = new AbortController();
      ctrl.abort();
      await assert.rejects(a.syncOnce({ signal: ctrl.signal }), { code: 'Cancelled' });
      assert.deepEqual(await a.status(), before);
      await assert.rejects(b.downloadAttachment(input.hash, { signal: ctrl.signal }), {
        code: 'Cancelled',
      });
      assert.equal((await b.attachmentStatus(input.hash)).state, 'Missing');
      const recoveryStart = (await trace()).length;
      assert.equal((await a.syncOnce()).healthy, true);
      assert.deepEqual(await wires(dataset), expected);
      const remote = await httpGet(attachmentKey(dataset, input.hash));
      assert.equal(sha(remote), input.hash);
      assert.deepEqual(remote, await readFile(input.path));
      const proof = await multipartProof(dataset, input, (await trace()).slice(recoveryStart));
      await b.syncOnce();
      assert.deepEqual(await b.read('SELECT * FROM items'), [row('one', input.hash)]);
      assert.equal((await b.attachmentStatus(input.hash)).state, 'Missing');
      assert.deepEqual(await b.downloadAttachment(input.hash), attachment);
      const exported = join(root, 'fault-export');
      await b.exportAttachment(input.hash, exported);
      assert.deepEqual(await readFile(exported), remote);
      assert.equal((await b.status()).pendingOutbox, '0');
      await noSecrets(backups, [await a.info(), await a.events(), await b.events()]);
      await dbNoSecrets(options.path);
      await artifact('S5-recovery', {
        identity,
        proof,
        expected,
        exportSHA256: sha(await readFile(exported)),
        final: await a.status(),
        peer: await b.status(),
        pendingOriginalBytes: true,
      });
    } finally {
      if (a) await a.close();
      if (b) await b.close();
    }
  },
);
