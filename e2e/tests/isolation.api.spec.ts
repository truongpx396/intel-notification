import { eventually, expect, Inbox, notify, test } from '../support/fixtures.js';

// NS-001 — a release blocker. The REST surface never shows a recipient another
// recipient's, tenant's or realm's notification (rest-api.md § Observable assertions).
//
// Pending: enable when the REST surface (T044) and service mode (T049) exist.
test.describe.fixme('recipient isolation over REST', () => {
  test('the same recipient id in two tenants sees only its own tenant', async ({ request, recipients }) => {
    const w1 = recipients.tenant('w1');
    const w2 = recipients.tenant('w2');
    const inW1 = new Inbox(request, recipients.in(w1, 'u1'));
    const inW2 = new Inbox(request, recipients.in(w2, 'u1')); // same recipient id, other tenant

    await notify(request, w1, [inW1.id.recipient], 'ingestion_complete', recipients.idemKey('w1-only'));

    await eventually(async () => expect(await inW1.list()).toHaveLength(1));
    expect(await inW2.list()).toHaveLength(0);
    expect((await inW2.unread()).count).toBe(0);
  });

  test("reading another recipient's notification is a 404 that changes nothing", async ({ request, recipients }) => {
    const tenant = recipients.tenant();
    const owner = new Inbox(request, recipients.in(tenant, 'owner'));
    const other = new Inbox(request, recipients.in(tenant, 'other'));
    await notify(request, tenant, [owner.id.recipient], 'ingestion_complete', recipients.idemKey('mine'));

    let items: Array<{ id: string }> = [];
    await eventually(async () => {
      items = await owner.list();
      expect(items).toHaveLength(1);
    });
    const res = await other.read(items[0]!.id);
    expect(res.status(), '404, not 403: a 403 would confirm the id exists').toBe(404);
    expect((await owner.unread()).count).toBe(1);
  });

  test('a request without a valid recipient token is refused', async ({ request }) => {
    for (const authorization of ['', 'Bearer not-a-jwt']) {
      const res = await request.get('/notifications', { headers: authorization ? { authorization } : {} });
      expect(res.status()).toBe(401);
    }
  });
});
