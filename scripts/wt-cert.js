'use strict';

// A certificate a browser accepts for WebTransport through
// serverCertificateHashes: ECDSA P-256, valid for at most 14 days, hashed
// as the SHA-256 of its DER. Made with the openssl CLI (Node's crypto can
// sign but not issue an X.509), written next to each other, the hash
// printed in the two spellings a page and a Node client want.
//
//   node scripts/wt-cert.js [dir] [--host 127.0.0.1] [--days 13]
//
// Writes <dir>/wt-key.pem, <dir>/wt-cert.pem and <dir>/wt-cert.json
// ({ hash, algorithm, notAfter }); dir defaults to ./certs. Used by
// examples/wt and the WRPC_WT= integration tests; never by the package.

const { execFileSync } = require('node:child_process');
const { createHash, X509Certificate } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const at = args.indexOf(name);
  return at === -1 ? fallback : args[at + 1];
};
const dir = path.resolve(
  args.find((arg, i) => !arg.startsWith('--') && (i === 0 || !args[i - 1].startsWith('--'))) ?? 'certs',
);
const host = option('--host', '127.0.0.1');
const days = Number(option('--days', '13'));
if (!Number.isInteger(days) || days < 1 || days > 14) {
  console.error('--days must be 1..14: a browser refuses a longer-lived certificate through serverCertificateHashes');
  process.exit(1);
}

fs.mkdirSync(dir, { recursive: true });
const keyPath = path.join(dir, 'wt-key.pem');
const certPath = path.join(dir, 'wt-cert.pem');
const infoPath = path.join(dir, 'wt-cert.json');

execFileSync(
  'openssl',
  [
    'req',
    '-x509',
    '-nodes',
    '-newkey',
    'ec',
    '-pkeyopt',
    'ec_paramgen_curve:prime256v1',
    '-keyout',
    keyPath,
    '-out',
    certPath,
    '-days',
    String(days),
    '-subj',
    `/CN=${host}`,
    '-addext',
    `subjectAltName=${/^[\d.]+$|^[0-9a-f:]+$/i.test(host) ? 'IP' : 'DNS'}:${host}`,
  ],
  { stdio: ['ignore', 'ignore', 'inherit'] },
);

const x509 = new X509Certificate(fs.readFileSync(certPath));
const hash = createHash('sha256').update(x509.raw).digest();
const info = {
  algorithm: 'sha-256',
  hash: hash.toString('base64'),
  hex: hash.toString('hex').match(/../g).join(':').toUpperCase(),
  host,
  notAfter: x509.validTo,
};
fs.writeFileSync(infoPath, `${JSON.stringify(info, null, 2)}\n`);

console.log(`key   ${keyPath}`);
console.log(`cert  ${certPath}  (valid until ${info.notAfter})`);
console.log(`hash  ${info.hash}`);
console.log(
  `\n  new WebTransport(url, { serverCertificateHashes: [{ algorithm: 'sha-256', value: Uint8Array.fromBase64('${info.hash}') }] })`,
);
