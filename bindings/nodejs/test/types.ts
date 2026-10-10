import { Buffer } from 'node:buffer';
import {
  open,
  restore,
  value,
  decodeValue,
  SdkError,
  type OpenOptions,
  type Status,
  type SqlValue,
} from '../index.js';
const values: SqlValue[] = [
  value.integer(9223372036854775807n),
  value.integer('-9223372036854775808'),
  value.real(1.125),
  value.blob(Buffer.from([0, 255])),
  value.null(),
  value.text('null'),
];
for (const v of values) decodeValue(v);
// @ts-expect-error integers require BigInt or decimal strings, never Number.
value.integer(9007199254740992);
// @ts-expect-error raw numeric params are not SQL values.
const invalid: SqlValue = 42;
async function contract(options: OpenOptions) {
  const db = await open(options);
  try {
    const status: Status = await db.status();
    await db.writeTransaction([
      { sql: 'INSERT INTO items VALUES (?,?)', params: [value.text('id'), value.null()] },
    ]);
    const c = new AbortController();
    await db.syncOnce({ signal: c.signal });
    for (const conflict of await db.listConflicts())
      await db.resolve({
        table: conflict.table,
        key: conflict.key,
        observed: conflict.heads.map((h) => h.id),
        row: null,
      });
    const attachment = await db.addAttachment('/host/input');
    await db.exportAttachment(attachment.hash, '/host/new-output');
    const manifest = await db.backup('/host/new-backup', 'LocalDisaster');
    const copy = await restore({
      backup: '/host/new-backup',
      destination: '/host/new-copy',
      backend: options.backend,
    });
    await copy.close();
    return { status, manifest };
  } catch (e) {
    if (e instanceof SdkError) return e.code;
    throw e;
  } finally {
    await db.close();
  }
}
void contract;
void invalid;
const webdavBasic: OpenOptions['backend'] = {
  kind: 'Webdav',
  endpoint: 'https://dav.example.invalid/',
  credentials: { kind: 'Basic', username: 'host', password: 'snapshot' },
};
const webdavBearer: OpenOptions['backend'] = {
  kind: 'Webdav',
  endpoint: 'https://dav.example.invalid/',
  credentials: { kind: 'Bearer', token: 'snapshot' },
  allow_loopback_http: false,
};
// @ts-expect-error NH1 does not expose S3.
const s3: OpenOptions['backend'] = { kind: 'S3' };
// @ts-expect-error Drive is deferred.
const drive: OpenOptions['backend'] = { kind: 'GoogleDrive' };
// @ts-expect-error Explicit boolean required.
const unsafe: OpenOptions['backend'] = { ...webdavBasic, allow_loopback_http: 'true' };
void webdavBasic;
void webdavBearer;
void s3;
void drive;
void unsafe;
