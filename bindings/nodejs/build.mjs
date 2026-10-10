import { spawnSync } from 'node:child_process';
import { copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
const root = fileURLToPath(new URL('../../', import.meta.url));
if (process.platform !== 'linux')
  throw new Error('Source build script currently verified only on Linux');
const build = spawnSync('cargo', ['build', '--locked', '-p', 'lfs-node'], {
  cwd: root,
  stdio: 'inherit',
  env: process.env,
});
if (build.status !== 0) process.exit(build.status ?? 1);
copyFileSync(
  resolve(root, process.env.CARGO_TARGET_DIR ?? 'target', 'debug/liblfs_node.so'),
  fileURLToPath(new URL('lfs_node.node', import.meta.url)),
);
