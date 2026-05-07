# DPI-Helper Telegram Bridge

Deno Deploy WebSocket relay between DPI-Helper clients and Telegram's MTProto
WebSocket servers (`kws*.web.telegram.org`).

## Deploy in 90 seconds

1. Open <https://dash.deno.com/new_project>.
2. Sign in with GitHub if needed.
3. Click "Deploy from GitHub repository".
4. Pick repository: `ivanusachev7-collab/dpi-helper-tg-bridge`.
5. Branch: `master`. Entry point: `main.ts`. Install step: leave blank.
6. Click **Deploy**. After ~10 seconds you'll get a URL like
   `https://<random-name>.deno.dev`.
7. Add that URL to `dpi-helper-tg-config/current.json` under `worker_bridges`,
   bump `version`, commit. Clients will pick it up within 4 seconds.

## How it works

Client opens `wss://<bridge>.deno.dev/ws?dc=2`, bridge opens `wss://kws2.web.telegram.org/apiws`,
relays binary frames both ways. No auth, no logging, no state.

Health check: `GET /` returns JSON with timestamp.
