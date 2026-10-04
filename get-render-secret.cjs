const fs = require('fs');
const path = require('path');

const secretPath = path.join(__dirname, '../.secrets/firebase-service-account.json');
if (!fs.existsSync(secretPath)) {
  console.error('File not found:', secretPath);
  process.exit(1);
}

const content = fs.readFileSync(secretPath, 'utf8');
const base64 = Buffer.from(content).toString('base64');

console.log('\n=== Render.com Environment Variable ===');
console.log('Key:   FIREBASE_SERVICE_ACCOUNT');
console.log('Value: Copy either the raw JSON or this Base64 string below:\n');
console.log(base64);
console.log('\n=======================================\n');
