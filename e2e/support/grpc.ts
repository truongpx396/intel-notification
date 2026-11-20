import { connect, type ClientHttp2Session } from 'node:http2';

// A producer written in another language (NS-011): a minimal gRPC client for the notify.v1 service, over
// Node's built-in HTTP/2 and a protobuf encoder small enough to read. It holds no code generated from the
// proto and no dependency of its own: adding a gRPC package would change package-lock.json, which this
// repository does not let a change touch by hand, and the point of the scenario is that nothing Go is
// needed to call the service, not which library does the framing.
//
// It speaks only what the scenario needs: strings, booleans and messages, repeated fields, and
// google.protobuf.Struct with string values. The field numbers are api/proto/notify/v1/notify.proto's.

// ---------------------------------------------------------------------------------------- encoding

const varint = (n: number): Buffer => {
  const bytes: number[] = [];
  let v = BigInt(n);
  do {
    let b = Number(v & 0x7fn);
    v >>= 7n;
    if (v > 0n) b |= 0x80;
    bytes.push(b);
  } while (v > 0n);
  return Buffer.from(bytes);
};

const tag = (field: number, wire: number): Buffer => varint((field << 3) | wire);
const bytesField = (field: number, data: Buffer): Buffer => Buffer.concat([tag(field, 2), varint(data.length), data]);

export const str = (field: number, value: string | undefined): Buffer =>
  value === undefined || value === '' ? Buffer.alloc(0) : bytesField(field, Buffer.from(value, 'utf8'));
export const bool = (field: number, value: boolean | undefined): Buffer =>
  value ? Buffer.concat([tag(field, 0), varint(1)]) : Buffer.alloc(0);
export const int = (field: number, value: number | undefined): Buffer =>
  value ? Buffer.concat([tag(field, 0), varint(value)]) : Buffer.alloc(0);
export const msg = (field: number, ...parts: Buffer[]): Buffer => bytesField(field, Buffer.concat(parts));
export const strs = (field: number, values: string[] | undefined): Buffer =>
  Buffer.concat((values ?? []).map((v) => bytesField(field, Buffer.from(v, 'utf8'))));

/** google.protobuf.Struct with string values: Struct{fields = 1 (map<string, Value>)}, Value{string_value = 3}. */
export const struct = (field: number, values: Record<string, string>): Buffer =>
  msg(
    field,
    ...Object.entries(values).map(([k, v]) => msg(1, str(1, k), msg(2, bytesField(3, Buffer.from(v, 'utf8'))))),
  );

// ---------------------------------------------------------------------------------------- decoding

export type Fields = Map<number, Array<bigint | Buffer>>;

/** decode reads a message into its fields: varints as bigint, everything else as the bytes it is. */
export function decode(buf: Buffer): Fields {
  const out: Fields = new Map();
  let i = 0;
  const readVarint = (): bigint => {
    let shift = 0n;
    let value = 0n;
    for (;;) {
      const b = buf[i++];
      if (b === undefined) throw new Error('a varint ran past the end of the message');
      value |= BigInt(b & 0x7f) << shift;
      if ((b & 0x80) === 0) return value;
      shift += 7n;
    }
  };
  while (i < buf.length) {
    const key = Number(readVarint());
    const field = key >> 3;
    const wire = key & 7;
    let value: bigint | Buffer;
    if (wire === 0) value = readVarint();
    else if (wire === 2) {
      const len = Number(readVarint());
      value = buf.subarray(i, i + len);
      i += len;
    } else if (wire === 1) {
      value = buf.subarray(i, i + 8);
      i += 8;
    } else if (wire === 5) {
      value = buf.subarray(i, i + 4);
      i += 4;
    } else throw new Error(`wire type ${wire} is not one this client reads`);
    const list = out.get(field) ?? [];
    list.push(value);
    out.set(field, list);
  }
  return out;
}

export const text = (f: Fields, field: number): string => {
  const v = f.get(field)?.[0];
  return v instanceof Buffer ? v.toString('utf8') : '';
};
export const flag = (f: Fields, field: number): boolean => f.get(field)?.[0] === 1n;
export const number = (f: Fields, field: number): number => Number((f.get(field)?.[0] as bigint | undefined) ?? 0n);
export const messages = (f: Fields, field: number): Fields[] =>
  (f.get(field) ?? []).filter((v): v is Buffer => v instanceof Buffer).map(decode);
export const texts = (f: Fields, field: number): string[] =>
  (f.get(field) ?? []).filter((v): v is Buffer => v instanceof Buffer).map((b) => b.toString('utf8'));

// ------------------------------------------------------------------------------------------- gRPC

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

// google.rpc.Status{code = 1, message = 2, details = 3 (Any{type_url = 1, value = 2})}; ErrorInfo{reason = 1}.
function reasonOf(detailsBin: string | undefined): string {
  if (!detailsBin) return '';
  const status = decode(Buffer.from(detailsBin, 'base64'));
  for (const any of messages(status, 3)) {
    if (text(any, 1).endsWith('google.rpc.ErrorInfo')) return text(decode((any.get(2)?.[0] as Buffer) ?? Buffer.alloc(0)), 1);
  }
  return '';
}

export class Producer {
  private session?: ClientHttp2Session;

  /** token is the bearer token the producer's host signed; the realm is the service's binding of it, never ours. */
  constructor(
    private readonly addr: string,
    private readonly token: string,
  ) {}

  private connection(): ClientHttp2Session {
    if (!this.session || this.session.closed || this.session.destroyed) {
      this.session = connect(`http://${this.addr}`);
      this.session.on('error', () => undefined); // a failed call reports it; the next call reconnects
    }
    return this.session;
  }

  close(): void {
    this.session?.close();
  }

  /** call makes one unary call and returns the response message's fields. */
  call(path: string, request: Buffer): Promise<Fields> {
    return new Promise((resolve, reject) => {
      const frame = Buffer.concat([Buffer.from([0]), Buffer.alloc(4), request]);
      frame.writeUInt32BE(request.length, 1);
      const req = this.connection().request({
        ':method': 'POST',
        ':path': path,
        'content-type': 'application/grpc',
        te: 'trailers',
        authorization: `Bearer ${this.token}`,
      });
      const chunks: Buffer[] = [];
      let headers: Record<string, string | string[] | undefined> = {};
      let trailers: Record<string, string | string[] | undefined> = {};
      req.on('response', (h) => (headers = h));
      req.on('trailers', (t) => (trailers = t));
      req.on('data', (c: Buffer) => chunks.push(c));
      req.on('error', reject);
      req.on('end', () => {
        const get = (k: string): string | undefined => {
          const v = trailers[k] ?? headers[k];
          return Array.isArray(v) ? v[0] : v;
        };
        const status = Number(get('grpc-status') ?? '2');
        if (status !== 0) {
          reject(new GrpcError(path, status, decodeURIComponent(get('grpc-message') ?? ''), reasonOf(get('grpc-status-details-bin'))));
          return;
        }
        const body = Buffer.concat(chunks);
        resolve(body.length >= 5 ? decode(body.subarray(5, 5 + body.readUInt32BE(1))) : new Map());
      });
      req.end(frame);
    });
  }
}

// -------------------------------------------------------------- the messages of the scenario (notify.proto)

export interface Tenant {
  kind: string;
  id: string;
}

const tenantMsg = (field: number, t: Tenant): Buffer => msg(field, str(1, t.kind), str(2, t.id));

export class Catalog {
  constructor(private readonly p: Producer) {}

  upsertTopic(topic: string, defaultChannels: string[], opts: { priority?: string; fallback?: string[]; essential?: boolean } = {}): Promise<Fields> {
    return this.p.call(
      '/notify.v1.Catalog/UpsertTopic',
      Buffer.concat([str(1, topic), strs(2, defaultChannels), strs(3, opts.fallback), str(4, opts.priority), bool(5, opts.essential)]),
    );
  }

  /** putTemplate stores a new, inactive version and returns the number the service assigned. */
  async putTemplate(ref: string, channel: string, locale: string, subject: string, body: string): Promise<number> {
    const stored = await this.p.call('/notify.v1.Catalog/PutTemplate', Buffer.concat([str(2, ref), str(3, channel), str(4, locale), str(6, subject), str(7, body)]));
    return number(stored, 5);
  }

  activateTemplate(ref: string, channel: string, locale: string, version: number): Promise<Fields> {
    return this.p.call('/notify.v1.Catalog/ActivateTemplate', Buffer.concat([str(2, ref), str(3, channel), str(4, locale), int(5, version)]));
  }

  putAddresses(tenant: Tenant, recipient: Tenant, channel: string, values: string[]): Promise<Fields> {
    return this.p.call(
      '/notify.v1.Catalog/PutAddresses',
      Buffer.concat([tenantMsg(1, tenant), tenantMsg(2, recipient), str(3, channel), ...values.map((v) => msg(4, str(1, v)))]),
    );
  }
}

export interface Delivery {
  channel: string;
  state: string;
}

export class Notifier {
  constructor(private readonly p: Producer) {}

  async notify(n: { tenant: Tenant; recipient: Tenant; topic: string; data: Record<string, string>; idemKey: string }): Promise<{ applied: boolean; channels: string[] }> {
    const receipt = await this.p.call(
      '/notify.v1.Notifier/Notify',
      Buffer.concat([tenantMsg(1, n.tenant), tenantMsg(2, n.recipient), str(3, n.topic), struct(5, n.data), str(9, n.idemKey)]),
    );
    return { applied: flag(receipt, 2), channels: texts(receipt, 3) };
  }

  async status(ref: { tenant: Tenant; recipient: Tenant; idemKey: string }): Promise<{ found: boolean; deliveries: Delivery[] }> {
    const s = await this.p.call('/notify.v1.Notifier/Status', Buffer.concat([tenantMsg(1, ref.tenant), tenantMsg(2, ref.recipient), str(3, ref.idemKey)]));
    return { found: flag(s, 1), deliveries: messages(s, 4).map((d) => ({ channel: text(d, 1), state: text(d, 3) })) };
  }
}
