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

  upstream.onopen = () => {
    upstreamReady = true;
    while (clientPending.length > 0) {
      const buf = clientPending.shift()!;
      try {
        upstream.send(buf);
      } catch (_) {
        /* ignore */
      }
    }
  };
  upstream.onmessage = (ev) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(ev.data);
    }
  };
  upstream.onclose = () => {
    if (client.readyState === WebSocket.OPEN) {
      client.close();
    }
  };
  upstream.onerror = () => {
    if (client.readyState === WebSocket.OPEN) {
      client.close(1011, "upstream error");
    }
  };

  client.onmessage = (ev) => {
    const data = ev.data instanceof ArrayBuffer
      ? ev.data
      : (ev.data as Uint8Array).buffer;
    if (!upstreamReady) {
      clientPending.push(data);
      return;
    }
    try {
      upstream.send(data);
    } catch (_) {
      /* ignore */
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
