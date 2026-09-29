import { generateKeyPairSync } from 'node:crypto';
import { createServer } from 'node:http';

// The service verifies recipient tokens against the host's JWKS (docs/security.md).
// In e2e the test run IS the host: it generates a signing key, serves its public
// half as a JWKS, and hands the private half to the workers through the
// environment. Nothing is written to disk and no key is committed.
//
// The compose stack points NOTIFY_RECIPIENT_JWKS at this server
// (host.docker.internal). The service fetches the JWKS on first use, so it does
// not matter whether the stack or this setup starts first.

const port = Number(process.env.NOTIFY_E2E_JWKS_PORT ?? 8099);

export default async function globalSetup(): Promise<() => Promise<void>> {
  const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'P-256' });
  const jwk = { ...publicKey.export({ format: 'jwk' }), kid: 'e2e', use: 'sig', alg: 'ES256' };

  const server = createServer((req, res) => {
    if (req.url === '/jwks.json') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((resolve) => server.listen(port, resolve));

  process.env.NOTIFY_E2E_SIGNING_KEY = privateKey.export({ format: 'pem', type: 'pkcs8' }).toString();
  process.env.NOTIFY_E2E_SIGNING_KID = 'e2e';

  return async () => {
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  };
}
