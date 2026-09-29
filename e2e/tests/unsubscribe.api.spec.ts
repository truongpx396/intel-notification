import { eventually, expect, Inbox, notify, test } from '../support/fixtures.js';

// D34 — RFC 8058 one-click unsubscribe. Mail goes to the Mailpit capture in the
// compose stack's e2e profile; the test reads it back through Mailpit's API.
const mailpit = process.env.NOTIFY_E2E_MAILPIT_URL ?? 'http://localhost:8025';

async function unsubscribeLink(request: import('@playwright/test').APIRequestContext, to: string): Promise<string> {
  let link = '';
  await eventually(async () => {
    const search = await request.get(`${mailpit}/api/v1/search?query=${encodeURIComponent(`to:${to}`)}`);
    const { messages } = await search.json();
    expect(messages.length).toBeGreaterThan(0);
    const headers = await (await request.get(`${mailpit}/api/v1/message/${messages[0].ID}/headers`)).json();
    expect(headers['List-Unsubscribe-Post']?.[0]).toBe('List-Unsubscribe=One-Click');
    link = /<([^>]+)>/.exec(headers['List-Unsubscribe'][0])![1]!;
  });
  return link;
}

// Pending: enable when the REST surface (T044), the email channel (T026) and
// service mode (T049) exist.
test.describe.fixme('one-click unsubscribe', () => {
  test('a GET of the link changes nothing; the one-click POST disables exactly one pair', async ({ request, recipients }) => {
    const inbox = new Inbox(request, recipients.in(recipients.tenant()));
    const email = `${inbox.id.recipient.id}@example.test`;
    // The recipient's address, as a producer registers it (Catalog.PutAddresses).
    await request.put('/admin/recipients/addresses', {
      data: { tenant: inbox.id.tenant, recipient: inbox.id.recipient, channel: 'email', addresses: [{ value: email }] },
    });
    await notify(request, inbox.id.tenant, [inbox.id.recipient], 'invite_received', recipients.idemKey('invite'));
    const link = await unsubscribeLink(request, email);

    // A mail scanner prefetching the link must not unsubscribe anyone.
    expect((await request.get(link)).ok()).toBeTruthy();
    const before = await (await inbox.preferences()).json();
    expect(before.find((p: { topic: string; channel: string }) =>
      p.topic === 'invite_received' && p.channel === 'email').enabled).toBe(true);

    const post = await request.post(link, { form: { 'List-Unsubscribe': 'One-Click' } });
    expect(post.ok()).toBeTruthy();
    const after = await (await inbox.preferences()).json();
    for (const p of after as Array<{ topic: string; channel: string; enabled: boolean }>) {
      const target = p.topic === 'invite_received' && p.channel === 'email';
      expect(p.enabled, `${p.topic}/${p.channel}`).toBe(target ? false : before.find(
        (b: { topic: string; channel: string }) => b.topic === p.topic && b.channel === p.channel).enabled);
    }
  });
});
