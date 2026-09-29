import { eventually, expect, Inbox, notify, test } from '../support/fixtures.js';

// NR-011, NR-012, NR-015, D26, D30 (rest-api.md § Preferences and schedule).
//
// Pending: enable when the REST surface (T044) and service mode (T049) exist.
test.describe.fixme('preferences over REST', () => {
  test('each preference reports where its value came from', async ({ request, recipients }) => {
    const inbox = new Inbox(request, recipients.in(recipients.tenant()));
    const res = await inbox.preferences();
    expect(res.ok()).toBeTruthy();
    for (const pref of await res.json()) {
      expect(['recipient', 'tenant', 'topic_default']).toContain(pref.source);
      expect(typeof pref.locked).toBe('boolean');
    }
  });

  test('an essential topic cannot be disabled, and the refusal says why', async ({ request, recipients }) => {
    const inbox = new Inbox(request, recipients.in(recipients.tenant()));
    const res = await inbox.setPreferences([{ topic: 'credit_exhausted', channel: 'email', enabled: false }]);
    expect(res.status()).toBe(422);
    expect(await res.text()).toContain('credit_exhausted');
  });

  test('with every inbox-backed channel off, the row exists but is not listed or counted', async ({ request, recipients }) => {
    const inbox = new Inbox(request, recipients.in(recipients.tenant()));
    expect((await inbox.setPreferences([{ topic: 'ingestion_complete', channel: 'in_app', enabled: false }])).ok()).toBeTruthy();

    await notify(request, inbox.id.tenant, [inbox.id.recipient], 'ingestion_complete', recipients.idemKey('hidden'));

    // Give delivery the NS-004 budget, then assert it stayed out of the inbox.
    await new Promise((resolve) => setTimeout(resolve, 5_000));
    expect(await inbox.list()).toHaveLength(0);
    expect((await inbox.unread()).count).toBe(0);
  });

  test('a replayed broadcast notifies nobody twice', async ({ request, recipients }) => {
    const inbox = new Inbox(request, recipients.in(recipients.tenant()));
    const key = recipients.idemKey('once');
    await notify(request, inbox.id.tenant, [inbox.id.recipient], 'ingestion_complete', key);
    await notify(request, inbox.id.tenant, [inbox.id.recipient], 'ingestion_complete', key);
    await eventually(async () => expect(await inbox.list()).toHaveLength(1));
  });
});
