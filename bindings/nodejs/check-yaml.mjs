import { readFileSync } from 'node:fs';
import { parseDocument } from 'yaml';

const files = process.argv.slice(2);
if (files.length === 0) throw new Error('Zero YAML files');
for (const file of files) {
  const document = parseDocument(readFileSync(file, 'utf8'), { uniqueKeys: true });
  if (document.errors.length || document.warnings.length) {
    throw new Error(`${file}: ${[...document.errors, ...document.warnings].join('\n')}`);
  }
  console.log(`YAML OK: ${file}`);
}
