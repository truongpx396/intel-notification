import type { Page } from '@playwright/test';

import { expect, type Identity, notify, recipientToken, test } from '../support/fixtures.js';

// D16 / NS-001 on the live path: the SSE stream, through a real browser's
// EventSource. The first design keyed this path "notify:user:<id>" and leaked
// across tenants; these are the tests that would have caught it.

interface StreamEvent {
  type: string;
  data: string;
}

// openStream connects an EventSource as the given recipient and collects events
// into window.__events. EventSource cannot set headers, so the token travels as
// a query parameter, which the relay accepts only on this route.
async function openStream(page: Page, id: Identity): Promise<void> {
  await page.goto('about:blank');
  await page.evaluate(
    ({ base, token }) => {
      const events: StreamEvent[] = [];
      (window as unknown as { __events: StreamEvent[] }).__events = events;
      const source = new EventSource(`${base}/notifications/stream?token=${encodeURIComponent(token)}`);
      for (const type of ['count', 'notification']) {
        source.addEventListener(type, (e) => events.push({ type, data: (e as MessageEvent).data }));
      }
    },
    { base: test.info().project.use.baseURL!, token: recipientToken(id) },
  );
}

const events = (page: Page) =>
  page.evaluate(() => (window as unknown as { __events: StreamEvent[] }).__events);

// Pending: enable when the SSE relay (T044) and service mode (T049) exist.
test.describe.fixme('live stream isolation', () => {
  test('the same recipient id in two tenants each sees only its own stream', async ({ browser, request, recipients }) => {
    const w1 = recipients.tenant('w1');
    const w2 = recipients.tenant('w2');
    const a = recipients.in(w1, 'u1');
    const b = recipients.in(w2, 'u1');
    const [pageA, pageB] = [await (await browser.newContext()).newPage(), await (await browser.newContext()).newPage()];
    await Promise.all([openStream(pageA, a), openStream(pageB, b)]);

    await notify(request, w1, [a.recipient], 'ingestion_complete', recipients.idemKey('live'));

    await expect.poll(async () => (await events(pageA)).filter((e) => e.type === 'notification').length, {
      timeout: 5_000, // NS-004
    }).toBe(1);
    // Give the other stream the same budget; it must stay silent.
    await pageB.waitForTimeout(2_000);
    expect((await events(pageB)).filter((e) => e.type === 'notification')).toHaveLength(0);
  });

  test('a (re)connect sends the bounded authoritative count before anything else', async ({ page, request, recipients }) => {
    const id = recipients.in(recipients.tenant());
    await notify(request, id.tenant, [id.recipient], 'ingestion_complete', recipients.idemKey('before-connect'));
    await page.waitForTimeout(2_000); // delivered while nobody was connected

    await openStream(page, id);
    await expect.poll(async () => (await events(page))[0]?.type).toBe('count');
    const first = JSON.parse((await events(page))[0]!.data);
    expect(first).toEqual({ count: 1, capped: false });
  });
});
