# @jsklabs-works/one-msg-ui-dist

Pre-built static files for [one-msg-ui](https://github.com/jsklabs-works/one-msg-ui)'s dashboard — the compiled output of `ui/`'s `npm run build` (HTML/JS/CSS), and nothing else. **No backend, no adapters, not runnable on its own.** If you just want to run the whole app, use the [Docker image](https://github.com/jsklabs-works/one-msg-ui#packaging-for-distribution) instead — this package exists for the narrower case of self-hosting the frontend against a one-msg-ui API you're already running somewhere.

## Install

```bash
npm install @jsklabs-works/one-msg-ui-dist
```

## What you get

Everything under `dist/` — `index.html`, hashed JS/CSS bundles, and a few static assets (favicon, etc.). No `main`/`exports` entry point to `require()`: this package's only job is putting files on disk under `node_modules`, the same pattern as packages like `swagger-ui-dist`.

## Serving it

Point any static file server at `node_modules/@jsklabs-works/one-msg-ui-dist/dist`. With Express, for example:

```js
const path = require("path");
const express = require("express");

const app = express();
const staticDir = path.join(require.resolve("@jsklabs-works/one-msg-ui-dist/package.json"), "..", "dist");
app.use(express.static(staticDir));

// SPA fallback for client-side routes (/brokers/:id, /resources/:id) —
// a hard refresh on one of those needs index.html, not a 404. See
// api/src/api/main.py's own serve_ui() for the reference implementation
// this mirrors.
app.get("*", (req, res) => res.sendFile(path.join(staticDir, "index.html")));

app.listen(3000);
```

## Pointing it at your API

The build sets `VITE_API_BASE_URL=""`, so every API call the UI makes is **relative to whatever origin serves these files** (`fetch("/api/brokers")`, not a hardcoded host). That means:

- **Same origin as your API** (reverse-proxied together, or your static server also proxies `/api/*` to the real API process) — works with no configuration at all.
- **Different origin** — your one-msg-ui API process needs CORS configured for the origin serving these static files. See [`api/src/api/main.py`](https://github.com/jsklabs-works/one-msg-ui/blob/main/api/src/api/main.py)'s `CORSMiddleware` setup; the checked-in default only allows `localhost:5173` (the dev server), so a real deployment needs its `allow_origins` widened to match wherever you're serving this package's files from.

## Versioning

Published versions track the main repo's release tags (`vX.Y.Z` → npm version `X.Y.Z`) — see the [main README's "Releasing a new version"](https://github.com/jsklabs-works/one-msg-ui#releasing-a-new-version) section. A given npm version's `dist/` is exactly what that same git tag's Docker image serves.
