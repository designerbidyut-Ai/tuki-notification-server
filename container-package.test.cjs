const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// Check the files actually copied into Render's image, not merely present locally.
test('Docker includes the complete worker dependency graph', () => {
  const docker = fs.readFileSync(path.join(__dirname, 'Dockerfile'), 'utf8');
  const copied = new Set();
  for (const line of docker.split(/\r?\n/)) {
    if (!line.startsWith('COPY ')) continue;
    const parts = line.trim().split(/\s+/).slice(1);
    assert.equal(parts.pop(), './', 'Review packaging check when COPY destinations change');
    for (const file of parts) {
      if (file === 'package*.json') continue;
      assert(!/[?*]/.test(file) && file !== '.', 'Keep runtime files explicitly allowlisted');
      assert(fs.existsSync(path.join(__dirname, file)), 'Missing Docker source: ' + file);
      copied.add(file);
    }
  }
  assert.match(docker, /CMD\s+\["node",\s*"worker\.js"\]/);
  const pending = ['worker.js'], visited = new Set();
  while (pending.length) {
    const file = pending.pop();
    if (visited.has(file)) continue;
    visited.add(file);
    assert(copied.has(file), 'Required runtime module omitted from Docker: ' + file);
    const source = fs.readFileSync(path.join(__dirname, file), 'utf8');
    for (const [, request] of source.matchAll(/require\(\s*['"]([^'"]+)['"]\s*\)/g)) {
      if (!request.startsWith('.')) continue;
      const dependency = path.posix.normalize(path.posix.join(path.posix.dirname(file), request));
      assert(!dependency.startsWith('../'), 'Runtime dependency outside Docker context');
      pending.push(dependency);
    }
  }
  assert(visited.has('city-join.cjs'), 'City notifications must be part of the deployed worker');
});
