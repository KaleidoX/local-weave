import { createRequire } from 'node:module';
const native = createRequire(import.meta.url)('./lfs_node.node');
const codes = new Set([
  'Busy',
  'Closed',
  'Limit',
  'Invalid',
  'Schema',
  'ConflictRequiresResolution',
  'Cancelled',
  'Unsupported',
  'Auth',
  'Transient',
  'Missing',
  'Corrupt',
  'Blocked',
  'Io',
  'Constraint',
  'Forbidden',
]);
export class SdkError extends Error {
  constructor(code) {
    super(codes.has(code) ? code : 'Invalid');
    this.name = 'SdkError';
    this.code = this.message;
  }
}
function encoded(input) {
  try {
    const s = JSON.stringify(input);
    if (Buffer.byteLength(s) > 8 * 1024 * 1024) throw new SdkError('Limit');
    return s;
  } catch (e) {
    throw e instanceof SdkError ? e : new SdkError('Invalid');
  }
}
class Database {
  #handle;
  constructor(handle) {
    this.#handle = handle;
  }
  async #request(op, input = {}, options = {}) {
    const token = new native.CancelToken();
    const signal = options.signal;
    const abort = () => token.cancel();
    try {
      if (signal?.aborted) token.cancel();
      signal?.addEventListener('abort', abort, { once: true });
      return JSON.parse(await this.#handle.request(op, encoded(input), token));
    } catch (e) {
      throw new SdkError(e.message);
    } finally {
      signal?.removeEventListener('abort', abort);
    }
  }
  read(sql, params = []) {
    return this.#request('read', { sql, params });
  }
  writeTransaction(statements, attachments = [], options) {
    return this.#request('write', { statements, attachments }, options);
  }
  syncOnce(options) {
    return this.#request('sync', {}, options);
  }
  addAttachment(path, options) {
    return this.#request('attachmentAdd', path, options);
  }
  downloadAttachment(hash, options) {
    return this.#request('attachmentDownload', hash, options);
  }
  attachmentStatus(hash) {
    return this.#request('attachmentStatus', hash);
  }
  exportAttachment(hash, path, options) {
    return this.#request('attachmentExport', [hash, path], options);
  }
  listConflicts() {
    return this.#request('conflicts');
  }
  resolve(resolution) {
    return this.#request('resolve', resolution);
  }
  events() {
    return this.#request('events');
  }
  backup(destination, kind, options) {
    return this.#request('backup', [destination, kind], options);
  }
  info() {
    return this.#request('info');
  }
  status() {
    return this.#request('status');
  }
  close() {
    return this.#request('close');
  }
}
export async function open(options) {
  try {
    return new Database(await native.open(encoded(options)));
  } catch (e) {
    throw new SdkError(e.message);
  }
}

export const value = Object.freeze({
  null: () => ({ type: 'null' }),
  text: (v) => {
    if (typeof v !== 'string' || Buffer.byteLength(v) > 524288) throw new SdkError('Invalid');
    return { type: 'text', value: v };
  },
  integer: (v) => {
    if (
      typeof v !== 'bigint' &&
      !(typeof v === 'string' && /^-?(0|[1-9][0-9]*)$/.test(v) && v.length <= 20)
    )
      throw new SdkError('Invalid');
    const n = BigInt(v);
    if (n < -(1n << 63n) || n >= 1n << 63n || String(n) !== String(v))
      throw new SdkError('Invalid');
    return { type: 'integer', value: String(n) };
  },
  real: (v) => {
    if (typeof v !== 'number' || !Number.isFinite(v)) throw new SdkError('Invalid');
    const b = Buffer.alloc(8);
    b.writeDoubleBE(v);
    return { type: 'real', value: b.toString('hex') };
  },
  blob: (v) => {
    if (!Buffer.isBuffer(v) || v.length > 524288) throw new SdkError('Invalid');
    return { type: 'blob', value: v.toString('hex') };
  },
});
export function decodeValue(v) {
  if (!v || typeof v !== 'object') throw new SdkError('Invalid');
  switch (v.type) {
    case 'null':
      return null;
    case 'integer':
      return BigInt(value.integer(v.value).value);
    case 'text':
      return value.text(v.value).value;
    case 'blob':
      if (
        typeof v.value !== 'string' ||
        v.value.length > 1048576 ||
        !/^([0-9a-f]{2})*$/.test(v.value)
      )
        throw new SdkError('Invalid');
      return Buffer.from(v.value, 'hex');
    case 'real':
      if (typeof v.value !== 'string' || !/^[0-9a-f]{16}$/.test(v.value))
        throw new SdkError('Invalid');
      {
        const n = Buffer.from(v.value, 'hex').readDoubleBE();
        if (!Number.isFinite(n)) throw new SdkError('Invalid');
        return n;
      }
    default:
      throw new SdkError('Invalid');
  }
}

export async function restore(options, operation = {}) {
  const token = new native.CancelToken(),
    signal = operation.signal,
    abort = () => token.cancel();
  try {
    if (signal?.aborted) token.cancel();
    signal?.addEventListener('abort', abort, { once: true });
    return new Database(await native.restore(encoded(options), token));
  } catch (e) {
    throw new SdkError(e.message);
  } finally {
    signal?.removeEventListener('abort', abort);
  }
}
