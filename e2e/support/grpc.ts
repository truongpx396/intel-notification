import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { type ChannelCredentials, Client, credentials, loadPackageDefinition, Metadata, type ServiceError } from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import protobuf from 'protobufjs';

// A producer written in another language (NS-011): a gRPC client for the notify.v1 services, built the way a
// producer in any language builds one, from the contract's own proto. The proto is read at run time from
// api/proto, so there is no copy of the contract here that could drift from it and no code generated from
// Go: what the producer speaks is what `buf` lints and `buf breaking` guards.
//
// It is a library's gRPC (@grpc/grpc-js) and a library's protobuf (@grpc/proto-loader), not a client
// written by hand. The one thing read here by hand-written definition is google.rpc.Status, the error
// details a refusal carries, because the contract's proto does not import it and the producer branches on
// the reason inside it (an ErrorInfo).

const protoRoot = fileURLToPath(new URL('../../api/proto/', import.meta.url));
const definition = loadSync(join(protoRoot, 'notify/v1/notify.proto'), {
  keepCase: true, // the proto's own field names, so the code reads as the contract does
  longs: Number,
  defaults: true,
  oneofs: true,
  includeDirs: [protoRoot],
});

type ServiceClient = new (address: string, creds: ChannelCredentials) => Client;
const notify = (loadPackageDefinition(definition) as unknown as { notify: { v1: Record<string, ServiceClient> } }).notify.v1;

// google.rpc.Status{code = 1, message = 2, details = 3 (Any{type_url = 1, value = 2})}; ErrorInfo{reason = 1}.
const rpc = protobuf.parse(`
  syntax = "proto3";
  package google.rpc;
  message Any { string type_url = 1; bytes value = 2; }
  message Status { int32 code = 1; string message = 2; repeated Any details = 3; }
  message ErrorInfo { string reason = 1; string domain = 2; map<string, string> metadata = 3; }
`).root;
const Status = rpc.lookupType('google.rpc.Status');
const ErrorInfo = rpc.lookupType('google.rpc.ErrorInfo');

/** GrpcError is a call the service refused: its gRPC status, its message, and the reason a producer branches on. */
export class GrpcError extends Error {
  constructor(
    readonly method: string,
    readonly code: number,
    message: string,
    readonly reason: string,
  ) {
    super(`${method}: gRPC status ${code}${reason ? ` ${reason}` : ''}: ${message}`);
  }
}

function reasonOf(err: ServiceError): string {
  const details = err.metadata?.get('grpc-status-details-bin')[0];
  if (!(details instanceof Buffer)) return '';
  const status = Status.decode(details) as unknown as { details: Array<{ typeUrl: string; value: Uint8Array }> };
  for (const any of status.details) {
    if (any.typeUrl.endsWith('google.rpc.ErrorInfo')) return (ErrorInfo.decode(any.value) as unknown as { reason: string }).reason;
  }
  return '';
}

export class Producer {
  private readonly clients = new Map<string, Client>();

  /** token is the bearer token the producer's host signed; the realm is the service's binding of it, never ours. */
  constructor(
    private readonly addr: string,
    private readonly token: string,
  ) {}

  private client(service: string): Client {
    let c = this.clients.get(service);
    if (!c) {
      const Ctor = notify[service];
      if (!Ctor) throw new Error(`the contract has no service ${service}`);
      c = new Ctor(this.addr, credentials.createInsecure());
      this.clients.set(service, c);
    }
    return c;
  }

  close(): void {
    for (const c of this.clients.values()) c.close();
    this.clients.clear();
  }

  /** call makes one unary call, by service and method name as the proto spells them, and returns the response. */
  call<Res>(service: string, method: string, request: object): Promise<Res> {
    return new Promise((resolve, reject) => {
      const metadata = new Metadata();
      metadata.set('authorization', `Bearer ${this.token}`);
      const fn = (this.client(service) as unknown as Record<string, (...a: unknown[]) => void>)[method];
      if (!fn) {
        reject(new Error(`the contract has no method ${service}/${method}`));
        return;
      }
      fn.call(this.client(service), request, metadata, (err: ServiceError | null, response: Res) => {
        if (err) reject(new GrpcError(`/notify.v1.${service}/${method}`, err.code, err.details, reasonOf(err)));
        else resolve(response);
      });
    });
  }
}

// -------------------------------------------------------------- the messages of the scenario (notify.proto)

export interface Tenant {
  kind: string;
  id: string;
}

/** structOf is a google.protobuf.Struct of string values: Struct{fields: map<string, Value>}, Value{stringValue}. */
const structOf = (values: Record<string, string>): object => ({
  fields: Object.fromEntries(Object.entries(values).map(([k, v]) => [k, { stringValue: v }])),
});

export class Catalog {
  constructor(private readonly p: Producer) {}

  upsertTopic(topic: string, defaultChannels: string[], opts: { priority?: string; fallback?: string[]; essential?: boolean } = {}): Promise<object> {
    return this.p.call('Catalog', 'UpsertTopic', {
      topic,
      default_channels: defaultChannels,
      fallback: opts.fallback ?? [],
      default_priority: opts.priority ?? '',
      essential: opts.essential ?? false,
    });
  }

  /** putTemplate stores a new, inactive version and returns the number the service assigned. */
  async putTemplate(ref: string, channel: string, locale: string, subject: string, body: string): Promise<number> {
    const stored = await this.p.call<{ version: number }>('Catalog', 'PutTemplate', { template_ref: ref, channel, locale, subject, body });
    return stored.version;
  }

  activateTemplate(ref: string, channel: string, locale: string, version: number): Promise<object> {
    return this.p.call('Catalog', 'ActivateTemplate', { template_ref: ref, channel, locale, version });
  }

  putAddresses(tenant: Tenant, recipient: Tenant, channel: string, values: string[]): Promise<object> {
    return this.p.call('Catalog', 'PutAddresses', { tenant, recipient, channel, addresses: values.map((value) => ({ value })) });
  }
}

export interface Delivery {
  channel: string;
  state: string;
  providerMessageId: string;
  providerStatus: string;
}

export class Notifier {
  constructor(private readonly p: Producer) {}

  async notify(n: { tenant: Tenant; recipient: Tenant; topic: string; data: Record<string, string>; idemKey: string }): Promise<{ applied: boolean; channels: string[] }> {
    const receipt = await this.p.call<{ applied: boolean; channels: string[] }>('Notifier', 'Notify', {
      tenant: n.tenant,
      recipient: n.recipient,
      topic: n.topic,
      data: structOf(n.data),
      idem_key: n.idemKey,
    });
    return { applied: receipt.applied, channels: receipt.channels };
  }

  async status(ref: { tenant: Tenant; recipient: Tenant; idemKey: string }): Promise<{ found: boolean; deliveries: Delivery[] }> {
    const s = await this.p.call<{ found: boolean; deliveries: Array<{ channel: string; state: string; provider_message_id: string; provider_status: string }> }>('Notifier', 'Status', {
      tenant: ref.tenant,
      recipient: ref.recipient,
      idem_key: ref.idemKey,
    });
    return {
      found: s.found,
      deliveries: s.deliveries.map((d) => ({ channel: d.channel, state: d.state, providerMessageId: d.provider_message_id, providerStatus: d.provider_status })),
    };
  }
}
