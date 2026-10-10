import { open, restore, type S3Backend, type Backend, type OpenOptions } from '../index.js';
const s3: S3Backend = {
  kind: 'S3',
  endpoint: 'https://s3.example.invalid',
  region: 'host-region',
  bucket: 'host-bucket',
  credentials: { access_key_id: 'host-key', secret_access_key: 'host-secret' },
};
const temporary: S3Backend = {
  ...s3,
  credentials: { ...s3.credentials, session_token: 'host-snapshot' },
  allow_loopback_http: false,
};
const union: Backend = temporary;
// @ts-expect-error Present null is not an omitted token.
const nullToken: S3Backend = { ...s3, credentials: { ...s3.credentials, session_token: null } };
// @ts-expect-error S3 credentials have no kind discriminator.
const tagged: S3Backend = { ...s3, credentials: { ...s3.credentials, kind: 'Static' } };
// @ts-expect-error Session tokens are strings.
const numeric: S3Backend = { ...s3, credentials: { ...s3.credentials, session_token: 1 } };
// @ts-expect-error Endpoint, region, bucket and credentials are required.
const incomplete: S3Backend = { kind: 'S3' };
async function publicUnion(options: OpenOptions) {
  const db = await open({ ...options, backend: s3 });
  await db.close();
  const clone = await restore({
    backup: '/host/backup',
    destination: '/host/copy',
    backend: temporary,
  });
  await clone.close();
}
void union;
void nullToken;
void tagged;
void numeric;
void incomplete;
void publicUnion;
