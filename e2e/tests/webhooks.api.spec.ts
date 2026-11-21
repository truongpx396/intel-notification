import { createHmac, randomUUID } from 'node:crypto';

import { Notifier, Producer } from '../support/grpc.js';
import { eventually, expect, producerToken, putAddresses, test } from '../support/fixtures.js';

// rest-api.md § Provider callbacks (NR-032, D14): signatures are verified before the body is parsed, and an
// unverified callback mutates nothing. A forged bounce would otherwise suppress a real address, a denial of
// notification that looks like normal operation.
//
// Resend signs with Svix: HMAC-SHA256 over "<svix-id>.<svix-timestamp>.<body>", sent as "v1,<base64>". The compose
// stack gives realm local a throwaway secret (deploy/docker-compose.yml); the run signs with the same one.
const secret = process.env.NOTIFY_E2E_RESEND_WEBHOOK_SECRET ?? 'whsec_ZTJlLXJlc2VuZC13ZWJob29rLXNlY3JldC0wMTIzNDU2Nzg5';
const grpcAddr = process.env.NOTIFY_E2E_GRPC_ADDR ?? 'localhost:9090';
const mailpit = process.env.NOTIFY_E2E_MAILPIT_URL ?? 'http://localhost:8025';

function signed(body: string, opts: { secret?: string; at?: Date } = {}): Record<string, string> {
  const id = `msg_${randomUUID()}`;
  const timestamp = Math.floor((opts.at ?? new Date()).getTime() / 1000);
  const key = Buffer.from((opts.secret ?? secret).replace(/^whsec_/, ''), 'base64');
  const signature = createHmac('sha256', key).update(`${id}.${timestamp}.${body}`).digest('base64');
  return { 'content-type': 'application/json', 'svix-id': id, 'svix-timestamp': String(timestamp), 'svix-signature': `v1,${signature}` };
}

test.describe('provider callbacks', () => {
  test('an unsigned bounce is refused with an empty body', async ({ request }) => {
    const res = await request.post('/webhooks/email/resend', {
      data: { type: 'email.bounced', data: { to: ['victim@example.test'] } },
    });
    expect(res.status()).toBe(401);
    expect(await res.text()).toBe('');
  });

  test('a body that is not even JSON is refused before it is parsed', async ({ request }) => {
    const res = await request.post('/webhooks/email/resend', {
      headers: { 'content-type': 'application/json' },
      data: '{not json',
    });
    expect(res.status(), 'a 400 would mean the parser saw the body first').toBe(401);
  });

  test('a callback signed with another secret, or a day ago, is refused', async ({ request }) => {
    const body = JSON.stringify({ type: 'email.bounced', data: { email_id: 're_x', to: ['victim@example.test'], bounce: { type: 'Permanent' } } });
    const other = 'whsec_' + Buffer.from('some other endpoint secret').toString('base64');
    for (const headers of [signed(body, { secret: other }), signed(body, { at: new Date(Date.now() - 24 * 3600_000) })]) {
      const res = await request.post('/webhooks/email/resend', { headers, data: body });
      expect(res.status()).toBe(401);
      expect(await res.text()).toBe('');
    }
  });

  test('a signed hard bounce suppresses the address: the next notification is not sent', async ({ request, recipients }) => {
    const tenant = recipients.tenant();
    const who = recipients.in(tenant);
    const address = `${who.recipient.id}@example.test`;
    await putAddresses(request, who, 'email', [address]);

    const producer = new Producer(grpcAddr, producerToken());
    try {
      const notifier = new Notifier(producer);
      const send = (key: string) =>
        notifier.notify({ tenant, recipient: who.recipient, topic: 'invite_received', data: { label: key }, idemKey: recipients.idemKey(key) });
      const emailOf = async (key: string) => {
        const status = await notifier.status({ tenant, recipient: who.recipient, idemKey: recipients.idemKey(key) });
        return status.deliveries.find((d) => d.channel === 'email');
      };

      await send('first');
      let messageId = '';
      await eventually(async () => {
        const delivery = await emailOf('first');
        expect(delivery?.state).toBe('delivered');
        expect(delivery?.providerMessageId).toBeTruthy();
        messageId = delivery!.providerMessageId;
      });

      // The provider reports a permanent bounce of that message.
      const body = JSON.stringify({ type: 'email.bounced', data: { email_id: messageId, to: [address], bounce: { type: 'Permanent' } } });
      const res = await request.post('/webhooks/email/resend', { headers: signed(body), data: body });
      expect(res.status(), await res.text()).toBe(204);
      // Its status is recorded against the delivery it named, and a replay changes nothing.
      await eventually(async () => expect((await emailOf('first'))?.providerStatus).toBe('bounced'));
      expect((await request.post('/webhooks/email/resend', { headers: signed(body), data: body })).status()).toBe(204);

      // The address is suppressed, so the next notification to it is not sent.
      await send('second');
      await eventually(async () => expect((await emailOf('second'))?.state).toBe('suppressed'));
      const found = await (await request.get(`${mailpit}/api/v1/search?query=${encodeURIComponent(`to:${address}`)}`)).json();
      expect(found.messages, 'only the first notification was mailed').toHaveLength(1);
    } finally {
      producer.close();
    }
  });

  test('a signed callback for a message nobody sent is a 404, so the provider tries again', async ({ request }) => {
    const body = JSON.stringify({ type: 'email.delivered', data: { email_id: `re_${randomUUID()}`, to: ['nobody@example.test'] } });
    const res = await request.post('/webhooks/email/resend', { headers: signed(body), data: body });
    expect(res.status()).toBe(404);
  });
});
