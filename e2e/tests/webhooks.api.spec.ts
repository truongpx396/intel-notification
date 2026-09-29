import { expect, test } from '../support/fixtures.js';

// rest-api.md § Provider callbacks: signatures are verified before the body is
// parsed, and an unverified callback mutates nothing. A forged bounce would
// otherwise suppress a real address — a denial of notification that looks like
// normal operation.
//
// Pending: enable when the REST surface (T044) and the email channel (T026) exist.
test.describe.fixme('provider callbacks', () => {
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
});
