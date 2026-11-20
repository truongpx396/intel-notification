import { Catalog, GrpcError, Notifier, Producer } from '../support/grpc.js';
import { eventually, expect, Inbox, producerToken, test } from '../support/fixtures.js';

// NS-011 — a producer written in a language other than Go registers a topic and a template, stores a
// recipient's addresses and sends a notification delivered on two channels, using only the service's API,
// with no host code in the service process.
//
// The producer here is TypeScript, over the gRPC surface and a client that holds nothing generated from Go
// (support/grpc.ts). It is the one spec that drives a producer over gRPC, because that is what it is for;
// every other spec reaches producers through the operator route and needs no gRPC client.
//
// What it sees, it sees where it landed and not from what the service says of itself: the message at the
// mail capture (Mailpit), the recipient's inbox over REST with a token their host signed, and the
// delivery status over the producer's own API.
const grpcAddr = process.env.NOTIFY_E2E_GRPC_ADDR ?? 'localhost:9090';
const mailpit = process.env.NOTIFY_E2E_MAILPIT_URL ?? 'http://localhost:8025';

class ScenarioFailed extends Error {
  constructor(readonly step: string, cause: unknown) {
    super(`${step}: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
  }
}

interface Scenario {
  topic: string;
  tenant: { kind: string; id: string };
  recipient: { kind: string; id: string };
  email: string;
  idemKey: string;
  channels: string[]; // the channels the topic is registered on
  skipTemplates?: boolean;
}

// run is the scenario, one step after another. The first step that fails is named in the error, so a
// scenario that is made to fail can be shown to fail at the step that was taken away.
async function run(s: Scenario): Promise<void> {
  const producer = new Producer(grpcAddr, producerToken());
  const catalog = new Catalog(producer);
  const notifier = new Notifier(producer);
  const step = async <T>(name: string, f: () => Promise<T>): Promise<T> => {
    try {
      return await f();
    } catch (err) {
      throw new ScenarioFailed(name, err);
    }
  };
  try {
    await step('register the topic', () => catalog.upsertTopic(s.topic, s.channels));
    if (!s.skipTemplates) {
      for (const [channel, subject, body] of [
        ['email', 'Invoice {{ .number }} is overdue', 'Hello {{ .name }}, invoice {{ .number }} is overdue.'],
        ['in_app', 'Invoice {{ .number }} is overdue', '{{ .name }}, please pay.'],
      ] as const) {
        await step(`register the ${channel} template`, async () => {
          const version = await catalog.putTemplate(s.topic, channel, 'en', subject, body);
          await catalog.activateTemplate(s.topic, channel, 'en', version);
        });
      }
    }
    await step("store the recipient's addresses", () => catalog.putAddresses(s.tenant, s.recipient, 'email', [s.email]));
    await step('send one notification', async () => {
      const receipt = await notifier.notify({ tenant: s.tenant, recipient: s.recipient, topic: s.topic, data: { number: 'INV-42', name: 'Ada' }, idemKey: s.idemKey });
      if (!receipt.applied) throw new Error('the notification was not applied');
    });
    await step('see it delivered on two channels', async () => {
      const deadline = Date.now() + 20_000;
      for (;;) {
        const status = await notifier.status({ tenant: s.tenant, recipient: s.recipient, idemKey: s.idemKey });
        const states = Object.fromEntries(status.deliveries.map((d) => [d.channel, d.state]));
        if (states.in_app === 'delivered' && states.email === 'delivered') return;
        // A delivery that ended in anything but delivered has ended: stop waiting, and say what it was.
        if (Object.values(states).some((state) => state !== 'delivered' && state !== 'pending' && state !== '')) {
          throw new Error(`deliveries ended as ${JSON.stringify(states)}`);
        }
        if (Date.now() > deadline) throw new Error(`deliveries are ${JSON.stringify(states)} after 20s`);
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    });
  } finally {
    producer.close();
  }
}

test.describe('a producer in another language (NS-011)', () => {
  test('registers a topic and templates, stores an address, sends one notification, and sees it on two channels', async ({ request, recipients }) => {
    const tenant = recipients.tenant();
    const inbox = new Inbox(request, recipients.in(tenant));
    const topic = recipients.unique('invoice_overdue');
    const email = `${inbox.id.recipient.id}@example.test`;

    await run({ topic, tenant, recipient: inbox.id.recipient, email, idemKey: recipients.idemKey('invoice-42'), channels: ['in_app', 'email'] });

    // The message at the provider: the template rendered over the producer's data, to the address it stored,
    // with the headers RFC 8058 asks of a non-essential topic.
    let id = '';
    await eventually(async () => {
      const search = await request.get(`${mailpit}/api/v1/search?query=${encodeURIComponent(`to:${email}`)}`);
      const { messages } = await search.json();
      expect(messages).toHaveLength(1);
      expect(messages[0].Subject).toBe('Invoice INV-42 is overdue');
      id = messages[0].ID;
    });
    const headers = await (await request.get(`${mailpit}/api/v1/message/${id}/headers`)).json();
    expect(headers['List-Unsubscribe-Post']?.[0]).toBe('List-Unsubscribe=One-Click');
    const body = await (await request.get(`${mailpit}/api/v1/message/${id}`)).json();
    expect(body.HTML ?? body.Text).toContain('Hello Ada, invoice INV-42 is overdue.');

    // And in the recipient's inbox, rendered from the in-app template, over the REST surface.
    await eventually(async () => {
      const items = await inbox.list();
      expect(items).toHaveLength(1);
      expect(items[0]!.topic).toBe(topic);
    });
  });

  // Shown to be able to fail, at the step that was taken away.
  test('fails at the topic when a channel it names does not exist in the service', async ({ recipients }) => {
    const tenant = recipients.tenant();
    const who = recipients.in(tenant);
    const failure = await run({
      topic: recipients.unique('needs_sms'), tenant, recipient: who.recipient, email: `${who.recipient.id}@example.test`,
      idemKey: recipients.idemKey('k'), channels: ['in_app', 'sms'],
    }).then(() => undefined, (e: unknown) => e);
    expect(failure).toBeInstanceOf(ScenarioFailed);
    expect((failure as ScenarioFailed).step).toBe('register the topic');
    const cause = (failure as ScenarioFailed).cause;
    expect(cause).toBeInstanceOf(GrpcError);
    expect((cause as GrpcError).reason).toBe('UNKNOWN_CHANNEL');
  });

  test('fails at the delivery when the template registration is skipped', async ({ recipients }) => {
    const tenant = recipients.tenant();
    const who = recipients.in(tenant);
    const failure = await run({
      topic: recipients.unique('no_template'), tenant, recipient: who.recipient, email: `${who.recipient.id}@example.test`,
      idemKey: recipients.idemKey('k'), channels: ['in_app', 'email'], skipTemplates: true,
    }).then(() => undefined, (e: unknown) => e);
    expect(failure).toBeInstanceOf(ScenarioFailed);
    expect((failure as ScenarioFailed).step).toBe('see it delivered on two channels');
    expect((failure as ScenarioFailed).message).toContain('rejected');
  });
});
