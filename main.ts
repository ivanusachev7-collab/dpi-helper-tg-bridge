// Deno Deploy WebSocket bridge for DPI-Helper.
// Accepts an incoming WebSocket from the dpi-helper client and relays binary
// frames to/from Telegram's `kws*.web.telegram.org` WebSocket endpoints.
//
// Why a hand-rolled upstream client (instead of `new WebSocket(url)`):
// the standard WebSocket constructor in Deno does not let us set the
// `Origin` or `Sec-WebSocket-Protocol` headers. Telegram's `kws*` endpoints
// reject (or immediately close) connections that don't carry
// `Sec-WebSocket-Protocol: binary` and the right Origin. We replicate the
// exact same handshake that the official web client uses.

const TELEGRAM_WS_HOSTS: Record<number, string[]> = {
  1: ["pluto.web.telegram.org", "pluto-1.web.telegram.org"],
  2: ["venus.web.telegram.org", "venus-1.web.telegram.org"],
  3: ["aurora.web.telegram.org", "aurora-1.web.telegram.org"],
  4: ["vesta.web.telegram.org", "vesta-1.web.telegram.org"],
  5: ["flora.web.telegram.org", "flora-1.web.telegram.org"],
};

function pickHost(dc: number): string | null {
  const list = TELEGRAM_WS_HOSTS[dc];
  if (!list || list.length === 0) return null;
  return list[Math.floor(Math.random() * list.length)];
}

// ---------------------------------------------------------------------------
// Hand-rolled RFC 6455 client. Async iterator yields binary payloads.
// ---------------------------------------------------------------------------

const OP_CONTINUATION = 0x0;
const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xA;

function randomKey(): string {
  const buf = new Uint8Array(16);
  crypto.getRandomValues(buf);
  let bin = "";
  for (const b of buf) bin += String.fromCharCode(b);
  return btoa(bin);
}

class RawWs {
  conn: Deno.TlsConn;
  closed = false;
  private readBuf = new Uint8Array(0);

  constructor(conn: Deno.TlsConn) {
    this.conn = conn;
  }

  static async connect(host: string, path: string): Promise<RawWs> {
    const conn = await Deno.connectTls({ hostname: host, port: 443 });
    const key = randomKey();
    const req =
      `GET ${path} HTTP/1.1\r\n` +
      `Host: ${host}\r\n` +
      `Upgrade: websocket\r\n` +
      `Connection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${key}\r\n` +
      `Sec-WebSocket-Version: 13\r\n` +
      `Sec-WebSocket-Protocol: binary\r\n` +
      `Origin: https://web.telegram.org\r\n` +
      `User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n` +
      `\r\n`;
    await conn.write(new TextEncoder().encode(req));

    const ws = new RawWs(conn);
    // Read response headers until \r\n\r\n.
    const headerBuf: number[] = [];
    const tmp = new Uint8Array(1024);
    while (true) {
      const n = await conn.read(tmp);
      if (n === null) throw new Error("server closed before handshake");
      for (let i = 0; i < n; i++) headerBuf.push(tmp[i]);
      // search for \r\n\r\n
      const idx = findDoubleCrlf(headerBuf);
      if (idx >= 0) {
        const headerStr = String.fromCharCode(...headerBuf.slice(0, idx));
        const statusLine = headerStr.split("\r\n", 1)[0];
        const status = parseInt(statusLine.split(" ")[1] ?? "0", 10);
        if (status !== 101) {
          throw new Error(`upstream handshake status=${status}: ${statusLine}`);
        }
        // leftover bytes after \r\n\r\n belong to ws frame stream
        const leftover = headerBuf.slice(idx + 4);
        ws.readBuf = new Uint8Array(leftover);
        return ws;
      }
      if (headerBuf.length > 16 * 1024) {
        throw new Error("response headers too large");
      }
    }
  }

  private async readExact(n: number): Promise<Uint8Array> {
    const out = new Uint8Array(n);
    let off = 0;
    if (this.readBuf.length > 0) {
      const take = Math.min(this.readBuf.length, n);
      out.set(this.readBuf.subarray(0, take), 0);
      this.readBuf = this.readBuf.slice(take);
      off = take;
    }
    while (off < n) {
      const got = await this.conn.read(out.subarray(off));
      if (got === null) throw new Error("upstream closed mid-frame");
      off += got;
    }
    return out;
  }

  async recv(): Promise<Uint8Array | null> {
    while (true) {
      if (this.closed) return null;
      let hdr: Uint8Array;
      try {
        hdr = await this.readExact(2);
      } catch (_e) {
        return null;
      }
      const fin = (hdr[0] & 0x80) !== 0;
      const opcode = hdr[0] & 0x0F;
      const masked = (hdr[1] & 0x80) !== 0;
      let len = hdr[1] & 0x7F;
      let payloadLen: number;
      if (len === 126) {
        const ext = await this.readExact(2);
        payloadLen = (ext[0] << 8) | ext[1];
      } else if (len === 127) {
        const ext = await this.readExact(8);
        // Treat upper 4 bytes as 0 (frames < 4 GiB).
        payloadLen = ((ext[4] << 24) | (ext[5] << 16) | (ext[6] << 8) | ext[7]) >>> 0;
      } else {
        payloadLen = len;
      }
      let mask = new Uint8Array(0);
      if (masked) mask = await this.readExact(4);
      const payload = await this.readExact(payloadLen);
      if (masked) {
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      }
      if (!fin) throw new Error("fragmented frames not supported");
      switch (opcode) {
        case OP_BINARY:
        case OP_TEXT:
          return payload;
        case OP_CLOSE:
          this.closed = true;
          return null;
        case OP_PING:
          await this.sendFrame(OP_PONG, payload);
          continue;
        case OP_PONG:
          continue;
        case OP_CONTINUATION:
          throw new Error("unexpected continuation");
        default:
          throw new Error(`unknown opcode 0x${opcode.toString(16)}`);
      }
    }
  }

  async sendFrame(opcode: number, payload: Uint8Array): Promise<void> {
    if (this.closed) throw new Error("ws closed");
    const len = payload.length;
    const header: number[] = [];
    header.push(0x80 | (opcode & 0x0F));
    // client → server frames MUST be masked
    if (len < 126) {
      header.push(0x80 | len);
    } else if (len < 65536) {
      header.push(0x80 | 126);
      header.push((len >> 8) & 0xFF, len & 0xFF);
    } else {
      header.push(0x80 | 127);
      const high = Math.floor(len / 0x100000000);
      const low = len >>> 0;
      header.push(
        (high >> 24) & 0xFF, (high >> 16) & 0xFF, (high >> 8) & 0xFF, high & 0xFF,
        (low >> 24) & 0xFF, (low >> 16) & 0xFF, (low >> 8) & 0xFF, low & 0xFF,
      );
    }
    const mask = new Uint8Array(4);
    crypto.getRandomValues(mask);
    header.push(mask[0], mask[1], mask[2], mask[3]);
    const frame = new Uint8Array(header.length + payload.length);
    frame.set(header, 0);
    for (let i = 0; i < payload.length; i++) frame[header.length + i] = payload[i] ^ mask[i & 3];
    await this.conn.write(frame);
  }

  async send(payload: Uint8Array): Promise<void> {
    return this.sendFrame(OP_BINARY, payload);
  }

  async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      try { await this.sendFrame(OP_CLOSE, new Uint8Array(0)); } catch (_) { /* ignore */ }
    }
    try { this.conn.close(); } catch (_) { /* ignore */ }
  }
}

function findDoubleCrlf(arr: number[]): number {
  for (let i = 3; i < arr.length; i++) {
    if (arr[i - 3] === 0x0d && arr[i - 2] === 0x0a && arr[i - 1] === 0x0d && arr[i] === 0x0a) {
      return i - 3;
    }
  }
  return -1;
}

// ---------------------------------------------------------------------------
// Deno.serve handler
// ---------------------------------------------------------------------------

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  if (url.pathname === "/" || url.pathname === "/health") {
    return new Response(
      JSON.stringify({ ok: true, service: "dpi-helper-tg-bridge", ts: new Date().toISOString() }),
      { headers: { "content-type": "application/json" } },
    );
  }
  if (url.pathname !== "/ws") return new Response("Not found", { status: 404 });
  const upgrade = req.headers.get("upgrade") || "";
  if (upgrade.toLowerCase() !== "websocket") {
    return new Response("Expected WebSocket", { status: 426 });
  }

  const dcStr = url.searchParams.get("dc") ?? "2";
  const dc = parseInt(dcStr, 10);
  if (![1, 2, 3, 4, 5].includes(dc)) {
    return new Response("Invalid dc parameter", { status: 400 });
  }
  const host = pickHost(dc);
  if (!host) return new Response("No upstream host for dc " + dc, { status: 502 });

  const { socket: client, response } = Deno.upgradeWebSocket(req);
  client.binaryType = "arraybuffer";

  const id = Math.random().toString(36).slice(2, 8);
  let bytesUp = 0, bytesDown = 0, framesUp = 0, framesDown = 0;
  console.log(`[${id}] new client → dc${dc} → wss://${host}/apiws`);

  // Connect upstream BEFORE letting the client send anything.
  let upstream: RawWs;
  try {
    upstream = await RawWs.connect(host, "/apiws");
    console.log(`[${id}] upstream connected`);
  } catch (e) {
    console.log(`[${id}] upstream connect FAILED: ${e}`);
    queueMicrotask(() => client.close(1011, `upstream connect: ${e}`));
    return response;
  }

  // Pump upstream → client.
  (async () => {
    try {
      while (true) {
        const frame = await upstream.recv();
        if (frame === null) {
          console.log(`[${id}] upstream CLOSED gracefully | up=${bytesUp}b/${framesUp}f down=${bytesDown}b/${framesDown}f`);
          break;
        }
        framesDown++;
        bytesDown += frame.byteLength;
        if (framesDown <= 5) {
          console.log(`[${id}] DOWN frame #${framesDown}: ${frame.byteLength} bytes`);
        }
        if (client.readyState !== WebSocket.OPEN) break;
        // client.send accepts ArrayBufferView
        try { client.send(frame); } catch (e) { console.log(`[${id}] client send error: ${e}`); break; }
      }
    } catch (e) {
      console.log(`[${id}] upstream pump error: ${e}`);
    } finally {
      try { client.close(); } catch (_) { /* ignore */ }
    }
  })();

  // Pump client → upstream.
  client.onmessage = async (ev) => {
    let data: Uint8Array;
    if (ev.data instanceof ArrayBuffer) {
      data = new Uint8Array(ev.data);
    } else if (ev.data instanceof Uint8Array) {
      data = ev.data;
    } else if (ev.data && typeof (ev.data as Blob).arrayBuffer === "function") {
      data = new Uint8Array(await (ev.data as Blob).arrayBuffer());
    } else if (typeof ev.data === "string") {
      data = new TextEncoder().encode(ev.data);
    } else {
      console.log(`[${id}] client UP: unknown data type`);
      return;
    }
    framesUp++;
    bytesUp += data.byteLength;
    if (framesUp <= 5) {
      console.log(`[${id}] UP frame #${framesUp}: ${data.byteLength} bytes`);
    }
    try {
      await upstream.send(data);
    } catch (e) {
      console.log(`[${id}] upstream send error: ${e}`);
    }
  };
  client.onclose = () => {
    console.log(`[${id}] client CLOSE | up=${bytesUp}b/${framesUp}f down=${bytesDown}b/${framesDown}f`);
    upstream.close();
  };
  client.onerror = (e) => {
    console.log(`[${id}] client ERROR: ${e}`);
    upstream.close();
  };

  return response;
});
