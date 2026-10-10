import type { Buffer } from 'node:buffer';
export type SqlValue =
  | { type: 'null' }
  | { type: 'integer'; value: string }
  | { type: 'real'; value: string }
  | { type: 'text'; value: string }
  | { type: 'blob'; value: string };
export type ErrorCode =
  | 'Busy'
  | 'Closed'
  | 'Limit'
  | 'Invalid'
  | 'Schema'
  | 'ConflictRequiresResolution'
  | 'Cancelled'
  | 'Unsupported'
  | 'Auth'
  | 'Transient'
  | 'Missing'
  | 'Corrupt'
  | 'Blocked'
  | 'Io'
  | 'Constraint'
  | 'Forbidden';
export class SdkError extends Error {
  readonly code: ErrorCode;
  constructor(code: ErrorCode);
}
export interface Schema {
  version: number;
  tables: Array<{
    id: number;
    name: string;
    columns: Array<{ name: string; kind: 'Text' | 'Integer' | 'Real' | 'Blob'; nullable: boolean }>;
  }>;
}
export interface FsBackend {
  kind: 'Fs';
  root: string;
}
export type WebdavCredentials =
  { kind: 'Basic'; username: string; password: string } | { kind: 'Bearer'; token: string };
export interface WebdavBackend {
  kind: 'Webdav';
  endpoint: string;
  credentials: WebdavCredentials;
  allow_loopback_http?: boolean;
}
export interface S3Backend {
  kind: 'S3';
  endpoint: string;
  region: string;
  bucket: string;
  credentials: { access_key_id: string; secret_access_key: string; session_token?: string };
  allow_loopback_http?: boolean;
}
export type Backend = FsBackend | WebdavBackend | S3Backend;
export interface OpenOptions {
  path: string;
  identity: [string, string, string];
  schema: Schema;
  backend: Backend;
  baseline_import?: boolean;
}
export interface OperationOptions {
  signal?: AbortSignal;
}
export interface Statement {
  sql: string;
  params?: SqlValue[];
}
export interface Status {
  pendingOutbox: string;
  healthy: boolean;
  blockedStreams: string;
  outstandingObjects: string;
  conflicts: string;
}
export interface SyncReport {
  uploaded: string;
  downloaded: string;
  completeScan: boolean;
  healthy: boolean;
  status: Status;
}
export interface Conflict {
  table: number;
  key: string;
  heads: Array<{ id: string; row: SqlValue[] | null }>;
}
export interface Resolution {
  table: number;
  key: string;
  observed: string[];
  row: SqlValue[] | null;
  attachments?: string[];
}
export interface Attachment {
  hash: string;
  size: string;
  state: 'Available';
}
export interface AttachmentStatus {
  hash: string;
  size: string | null;
  state: string;
}
export type BackupKind = 'LocalDisaster' | 'SharedBootstrap';
export interface BackupManifest {
  format: number;
  protocol: number;
  internal: number[];
  kind: BackupKind;
  identity: [string, string, string];
  schema: Schema;
  files: Array<{ path: string; sha256: string; size: string }>;
}
export interface StatusEvent {
  operation: string;
  success: boolean;
  error: ErrorCode | null;
}
export interface Database {
  read(sql: string, params?: SqlValue[]): Promise<SqlValue[][]>;
  /**
   * Each explicit attachment hash must be locally installed/Available and pass a full SHA256
   * check before business SQL runs. A known Missing explicit hash currently rejects with Invalid;
   * business rows, sequence, causal state, outbox and status() remain unchanged (events may record failure).
   * A corrupt previously Available file may be marked Missing; file I/O errors use their own codes.
   * Install with addAttachment/downloadAttachment first; no network runs in the transaction.
   * For an existing row retaining its reference, ordinary edits may omit attachments (or use []),
   * even with a Missing local body: the full row and causal ancestor reference history are retained.
   * No inference from arbitrary strings or automatic inheritance for new unrelated rows.
   */
  writeTransaction(
    statements: Statement[],
    attachments?: string[],
    options?: OperationOptions,
  ): Promise<{ committed: boolean; status: Status }>;
  syncOnce(options?: OperationOptions): Promise<SyncReport>;
  status(): Promise<Status>;
  info(): Promise<{ identity: [string, string, string]; schema: Schema }>;
  events(): Promise<StatusEvent[]>;
  listConflicts(): Promise<Conflict[]>;
  /**
   * Every explicit resolution.attachments hash has the same local Available + full SHA256
   * precondition as writeTransaction. Install via addAttachment/downloadAttachment first.
   * A known Missing explicit hash rejects with Invalid before resolving: no business, sequence,
   * causal or outbox commit, and status() stays unchanged. Failure events may be recorded;
   * corrupt Available files may become Missing and I/O errors use their own codes.
   * No network in the transaction. Existing reference history remains without repeating all hashes;
   * row strings alone do not introduce references or associate new unrelated records.
   */
  resolve(resolution: Resolution): Promise<Status>;
  addAttachment(path: string, options?: OperationOptions): Promise<Attachment>;
  downloadAttachment(hash: string, options?: OperationOptions): Promise<Attachment>;
  attachmentStatus(hash: string): Promise<AttachmentStatus>;
  exportAttachment(
    hash: string,
    path: string,
    options?: OperationOptions,
  ): Promise<{ exported: true }>;
  backup(
    destination: string,
    kind: BackupKind,
    options?: OperationOptions,
  ): Promise<BackupManifest>;
  close(): Promise<{ closed: true }>;
}
export function open(options: OpenOptions): Promise<Database>;
export function restore(
  options: { backup: string; destination: string; backend: Backend },
  operation?: OperationOptions,
): Promise<Database>;
export const value: {
  null(): SqlValue;
  integer(v: bigint | string): SqlValue;
  real(v: number): SqlValue;
  text(v: string): SqlValue;
  blob(v: Buffer): SqlValue;
};
export function decodeValue(v: SqlValue): null | bigint | number | string | Buffer;
