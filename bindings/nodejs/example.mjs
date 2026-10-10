import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { open, restore, value as v } from './index.js';
const root = await mkdtemp(join(tmpdir(), 'lfs-node-example-'));
const schema = {
  version: 1,
  tables: [
    {
      id: 1,
      name: 'notes',
      columns: [
        { name: 'id', kind: 'Text', nullable: false },
        { name: 'body', kind: 'Text', nullable: false },
      ],
    },
  ],
};
const backend = { kind: 'Fs', root: join(root, 'remote') };
let a, b, copy;
try {
  await mkdir(backend.root);
  a = await open({
    path: join(root, 'a.db'),
    identity: ['example', 'a', 'first'],
    schema,
    backend,
  });
  b = await open({
    path: join(root, 'b.db'),
    identity: ['example', 'b', 'first'],
    schema,
    backend,
  });
  const insert = (id, body) => ({
    sql: 'INSERT INTO notes VALUES (?,?)',
    params: [v.text(id), v.text(body)],
  });
  await writeFile(join(root, 'file'), 'example attachment');
  const attachment = await a.addAttachment(join(root, 'file'));
  await a.writeTransaction(
    [insert('shared', 'initial'), insert('deleted', 'temporary')],
    [attachment.hash],
  );
  await a.syncOnce();
  await b.syncOnce();
  await b.downloadAttachment(attachment.hash);
  await b.exportAttachment(attachment.hash, join(root, 'downloaded'));
  assert.equal(await readFile(join(root, 'downloaded'), 'utf8'), 'example attachment');
  await a.writeTransaction([
    { sql: "DELETE FROM notes WHERE id='shared'" },
    { sql: "DELETE FROM notes WHERE id='deleted'" },
  ]);
  await b.writeTransaction([{ sql: "UPDATE notes SET body='offline edit' WHERE id='shared'" }]);
  await a.syncOnce();
  await b.syncOnce();
  await a.syncOnce();
  const [conflict] = await a.listConflicts();
  assert.equal(conflict.heads.length, 2);
  await assert.rejects(
    a.writeTransaction([{ sql: "UPDATE notes SET body='implicit' WHERE id='shared'" }]),
    { code: 'ConflictRequiresResolution' },
  );
  await a.resolve({
    table: 1,
    key: 'shared',
    observed: conflict.heads.map((h) => h.id),
    row: [v.text('shared'), v.text('explicit merged value')],
  });
  await a.syncOnce();
  await b.syncOnce();
  assert.deepEqual(await a.read('SELECT * FROM notes'), await b.read('SELECT * FROM notes'));
  assert.equal((await b.status()).pendingOutbox, '0');
  await a.backup(join(root, 'snapshot'), 'SharedBootstrap');
  copy = await restore({
    backup: join(root, 'snapshot'),
    destination: join(root, 'copy'),
    backend,
  });
  assert.notEqual((await copy.info()).identity[2], (await a.info()).identity[2]);
  console.log(
    'PASS actual Node native + OpenDAL Fs: offline insert/edit/delete, streamed attachment bytes, conflict/observed resolve/no echo, shared backup/rotated restore',
  );
} finally {
  for (const db of [a, b, copy]) if (db) await db.close();
  await rm(root, { recursive: true, force: true });
}
