import { createPrivateKey, randomUUID, sign } from 'node:crypto';

import { test as base, expect, type APIRequestContext } from '@playwright/test';

// The realm the compose stack binds its local producer and JWKS to.
export const realm = 'local';

export interface Tenant {
  kind: string;
  id: string;
}

export interface Recipient {
  kind: string;
  id: string;
}

export interface Identity {
  tenant: Tenant;
  recipient: Recipient;
}

const b64url = (data: Buffer | string): string => Buffer.from(data).toString('base64url');

// recipientToken mints what a host mints for its signed-in user: a short-lived
// ES256 JWT naming the tenant and recipient (rest-api.md § Authentication).
export function recipientToken(id: Identity, ttlSeconds = 600): string {
  const pem = process.env.NOTIFY_E2E_SIGNING_KEY;
  if (!pem) throw new Error('NOTIFY_E2E_SIGNING_KEY is unset: global setup did not run');
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'ES256', typ: 'JWT', kid: process.env.NOTIFY_E2E_SIGNING_KID };
  const claims = {
    iss: realm,
    tenant: id.tenant,
    recipient: id.recipient,
    iat: now,
    exp: now + ttlSeconds,
  };
  const input = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(claims))}`;
  // JWS ES256 is the raw r||s signature, not DER.
  const signature = sign('sha256', Buffer.from(input), {
    key: createPrivateKey(pem),
    dsaEncoding: 'ieee-p1363',
  });
  return `${input}.${b64url(signature)}`;
}

// Recipients builds identities unique to one test, so parallel tests never share
// a tenant, a recipient or an idempotency key. `same` deliberately reuses a
// recipient id in a second tenant: the shape that exposes an isolation bug.
export class Recipients {
  private readonly run = randomUUID().slice(0, 8);

  tenant(label = 't'): Tenant {
    return { kind: 'workspace', id: `${label}-${this.run}` };
  }

  in(tenant: Tenant, id = 'u1'): Identity {
    return { tenant, recipient: { kind: 'user', id: `${id}-${this.run}` } };
  }

  idemKey(label: string): string {
    return `${label}:${this.run}`;
  }
}

// Notify sends through the operator broadcast route with an inline recipient
// list, which is the producer path reachable over HTTP.
export async function notify(
  api: APIRequestContext,
  tenant: Tenant,
  recipients: Recipient[],
  topic: string,
  idemKey: string,
): Promise<void> {
  const res = await api.post('/admin/notifications/broadcast', {
    headers: { authorization: `Bearer ${process.env.NOTIFY_E2E_OPERATOR_TOKEN ?? 'e2e-operator'}` },
    data: { tenant, recipients, topic, data: { label: idemKey }, idem_key: idemKey },
  });
  expect(res.status(), await res.text()).toBe(202);
}

// Inbox is a recipient's view of the REST surface, authenticated as that recipient.
export class Inbox {
  constructor(
    private readonly api: APIRequestContext,
    readonly id: Identity,
  ) {}

  private headers(): Record<string, string> {
    return { authorization: `Bearer ${recipientToken(this.id)}` };
  }

  async list(): Promise<Array<{ id: string; topic: string }>> {
    const res = await this.api.get('/notifications', { headers: this.headers() });
    expect(res.ok(), await res.text()).toBeTruthy();
    return (await res.json()).items;
  }

  async unread(): Promise<{ count: number; capped: boolean }> {
    const res = await this.api.get('/notifications/unread-count', { headers: this.headers() });
    expect(res.ok(), await res.text()).toBeTruthy();
    return res.json();
  }

  read(notificationId: string) {
    return this.api.post(`/notifications/${notificationId}/read`, { headers: this.headers() });
  }

  preferences() {
    return this.api.get('/notifications/preferences', { headers: this.headers() });
  }

  setPreferences(prefs: Array<{ topic: string; channel: string; enabled: boolean }>) {
    return this.api.put('/notifications/preferences', { headers: this.headers(), data: prefs });
  }
}

// eventually polls until check stops throwing: delivery is asynchronous, and
// NS-004 bounds it at 5 seconds p95.
export async function eventually(check: () => Promise<void>, timeoutMs = 10_000): Promise<void> {
  await expect(check).toPass({ timeout: timeoutMs });
}

export const test = base.extend<{ recipients: Recipients }>({
  // Fixture functions receive fixtures by destructuring; this one needs none.
  // eslint-disable-next-line no-empty-pattern
  recipients: async ({}, use) => {
    await use(new Recipients());
  },
});

export { expect };
