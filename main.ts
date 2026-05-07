// Deno Deploy WebSocket bridge for DPI-Helper.
// Accepts an incoming WebSocket from the dpi-helper client, opens an upstream
// WebSocket connection to the requested Telegram DC, and relays binary frames
// in both directions.
//
// Connection URL shape (client →):
//   wss://<bridge>.deno.dev/ws?dc=2
//
// On open we look up the DC's upstream WebSocket endpoint (Telegram's
// `kws*.web.telegram.org` family) and connect to it. We then pipe every
// `MessageEvent.data` (binary) through unchanged.
//
// No auth — bridge only serves Telegram WS traffic. Cap concurrent connections
// per IP via Deno Deploy's built-in rate limit if needed (not enforced here).

const TELEGRAM_WS_HOSTS: Record<number, string[]> = {
  1: ["kws1.web.telegram.org", "kws1-1.web.telegram.org"],
  2: ["kws2.web.telegram.org", "kws2-1.web.telegram.org"],
  3: ["kws3.web.telegram.org", "kws3-1.web.telegram.org"],
  4: ["kws4.web.telegram.org", "kws4-1.web.telegram.org"],
  5: ["kws5.web.telegram.org", "kws5-1.web.telegram.org"],
};

function pickHost(dc: number): string | null {
  const list = TELEGRAM_WS_HOSTS[dc];
  if (!list || list.length === 0) return null;
  return list[Math.floor(Math.random() * list.length)];
}

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  // Health check.
  if (url.pathname === "/" || url.pathname === "/health") {
    return new Response(
      JSON.stringify({
        ok: true,
        service: "dpi-helper-tg-bridge",
        ts: new Date().toISOString(),
      }),
      { headers: { "content-type": "application/json" } },
    );
  }
  if (url.pathname !== "/ws") {
    return new Response("Not found", { status: 404 });
  }
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
  if (!host) {
    return new Response("No upstream host for dc " + dc, { status: 502 });
  }

  // Upgrade the client connection.
  const { socket: client, response } = Deno.upgradeWebSocket(req);
  client.binaryType = "arraybuffer";

  // Open upstream connection.
  const upstreamUrl = `wss://${host}/apiws`;
  let upstream: WebSocket;
  try {
    upstream = new WebSocket(upstreamUrl);
    upstream.binaryType = "arraybuffer";
  } catch (e) {
    queueMicrotask(() => client.close(1011, `upstream ctor: ${e}`));
    return response;
  }

  let upstreamReady = false;
  const clientPending: ArrayBuffer[] = [];

  const id = Math.random().toString(36).slice(2, 8);
  let bytesUp = 0, bytesDown = 0, framesUp = 0, framesDown = 0;
  console.log(`[${id}] new client → dc${dc} → ${upstreamUrl}`);

  upstream.onopen = () => {
    upstreamReady = true;
    console.log(`[${id}] upstream OPEN, flushing ${clientPending.length} pending`);
    while (clientPending.length > 0) {
      const buf = clientPending.shift()!;
      try {
        upstream.send(buf);
        bytesUp += (buf as ArrayBuffer).byteLength;
        framesUp++;
      } catch (e) {
        console.log(`[${id}] upstream send error on flush: ${e}`);
      }
    }
  };
  upstream.onmessage = async (ev) => {
    framesDown++;
    const dataType = ev.data?.constructor?.name ?? typeof ev.data;
    const bin = await toBinary(ev.data);
    const size = (bin as ArrayBuffer).byteLength;
    bytesDown += size;
    if (framesDown <= 5) {
      console.log(`[${id}] DOWN frame #${framesDown}: ${size} bytes (incoming type=${dataType})`);
    }
    if (client.readyState !== WebSocket.OPEN) {
      console.log(`[${id}] client not OPEN (state=${client.readyState}), dropping DOWN frame`);
      return;
    }
    try {
      client.send(bin);
    } catch (e) {
      console.log(`[${id}] client send error: ${e}`);
    }
  };
  upstream.onclose = (ev) => {
    console.log(`[${id}] upstream CLOSE code=${ev.code} reason=${ev.reason} | up=${bytesUp}b/${framesUp}f down=${bytesDown}b/${framesDown}f`);
    if (client.readyState === WebSocket.OPEN) {
      client.close();
    }
  };
  upstream.onerror = (e) => {
    console.log(`[${id}] upstream ERROR: ${e}`);
    if (client.readyState === WebSocket.OPEN) {
      client.close(1011, "upstream error");
    }
  };

  async function toBinary(d: unknown): Promise<ArrayBuffer | Uint8Array> {
    if (d instanceof ArrayBuffer) return d;
    if (d instanceof Uint8Array) {
      return d.slice().buffer;
    }
    // deno-lint-ignore no-explicit-any
    const anyD = d as any;
    if (anyD && typeof anyD.arrayBuffer === "function") {
      // Blob — Deno's outbound WebSocket may deliver binary as Blob even when
      // we set binaryType = "arraybuffer". Convert it.
      try {
        const ab = await anyD.arrayBuffer();
        return ab as ArrayBuffer;
      } catch (e) {
        console.log(`[${id}] Blob.arrayBuffer() failed: ${e}`);
      }
    }
    if (typeof d === "string") {
      return new TextEncoder().encode(d).buffer;
    }
    console.log(`[${id}] toBinary: UNKNOWN type, ctor=${(d as { constructor?: { name?: string } } | null)?.constructor?.name}`);
    return new ArrayBuffer(0);
  }

  client.onmessage = async (ev) => {
    const data = await toBinary(ev.data);
    const size = (data as ArrayBuffer).byteLength;
    if (!upstreamReady) {
      clientPending.push(data as ArrayBuffer);
      console.log(`[${id}] UP frame buffered (upstream not ready): ${size} bytes`);
      return;
    }
    framesUp++;
    bytesUp += size;
    if (framesUp <= 5) {
      console.log(`[${id}] UP frame #${framesUp}: ${size} bytes`);
    }
    try {
      upstream.send(data);
    } catch (e) {
      console.log(`[${id}] upstream send error: ${e}`);
    }
  };
  client.onclose = () => {
    if (
      upstream.readyState === WebSocket.OPEN ||
      upstream.readyState === WebSocket.CONNECTING
    ) {
      upstream.close();
    }
  };
  client.onerror = () => {
    try {
      upstream.close();
    } catch (_) {
      /* ignore */
    }
  };

  return response;
});
