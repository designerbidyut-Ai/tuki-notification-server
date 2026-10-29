const test = require('node:test');
const assert = require('node:assert/strict');
const {once} = require('node:events');
const {gzipSync} = require('node:zlib');
const express = require('express');

test('JSON middleware handles compressed trailing bytes without stopping the server', async () => {
  const app = express();
  app.post('/request', express.json({limit: '2kb'}), (_req, res) => res.sendStatus(401));
  app.get('/health', (_req, res) => res.json({ok: true}));
  app.use((error, _req, res, _next) => res.sendStatus(error.status || 400));
  const server = app.listen(0, '127.0.0.1');
  try {
    await once(server, 'listening');
    const base = 'http://127.0.0.1:' + server.address().port;
    const valid = gzipSync(Buffer.from('{"probe":true}'));
    for (const [label, body, encoding, statuses] of [
      ['plain JSON', Buffer.from('{}'), null, [401]],
      ['valid compressed JSON', valid, 'gzip', [401]],
      ['compressed JSON with trailing bytes', Buffer.concat([valid, Buffer.from('invalid trailing bytes')]), 'gzip', [400, 401]],
      ['malformed compressed body', Buffer.from('not a gzip frame'), 'gzip', [400]],
      ['oversized JSON', Buffer.from(JSON.stringify({text: 'x'.repeat(3000)})), null, [413]],
    ]) {
      const headers = {'Content-Type': 'application/json'};
      if (encoding) headers['Content-Encoding'] = encoding;
      const response = await fetch(base + '/request', {method: 'POST', headers, body, signal: AbortSignal.timeout(5000)});
      assert(statuses.includes(response.status), label + ': ' + response.status);
      await response.text();
      const health = await fetch(base + '/health', {signal: AbortSignal.timeout(5000)});
      assert.equal(health.status, 200, 'Server remains available after ' + label);
      assert.deepEqual(await health.json(), {ok: true});
    }
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});
