# Single-container distribution: the UI built as static files, served by
# the same FastAPI process as the API (see api/src/api/main.py's
# ONE_MSG_UI_STATIC_DIR block) — one image, one port, no Python/Node
# toolchain needed on the machine that runs it. See README.md's
# "Packaging for distribution" section for build/run instructions and the
# plain (non-Docker) build path.

# ---- Stage 1: build the UI --------------------------------------------
FROM node:20-alpine AS ui-build
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
# Empty, not omitted: api.ts's BASE_URL only falls back to localhost:8010
# on an *unset* env var (??, not ||) — an explicit empty string makes
# every request relative to whatever origin serves the built UI, which is
# this same container's own API once stage 2 serves both from one port.
ENV VITE_API_BASE_URL=""
RUN npm run build

# ---- Stage 2: the API + all adapters, plus the built UI ----------------
FROM python:3.12-slim AS app
WORKDIR /app

# Every adapter is pure-Python (requests, plus solace-pubsubplus's
# prebuilt wheel for Solace's message-peek client — see
# adapters/solace/README.md) — no compiler toolchain needed to install any
# of them, verified by this image actually building.
COPY adapters/ ./adapters/
COPY api/ ./api/
RUN pip install --no-cache-dir \
      ./adapters/kafka ./adapters/solace ./adapters/mq ./adapters/rabbitmq ./adapters/activemq \
      ./api

COPY --from=ui-build /ui/dist ./ui/dist
ENV ONE_MSG_UI_STATIC_DIR=/app/ui/dist
# Not baked into the image on purpose — config/brokers.json under this
# path lists real broker connection details (see api/README.md's Broker
# configuration section). Mount a volume at /data (docker run -v
# $(pwd)/data:/data ...) to persist brokers added through the UI itself;
# an unmounted /data just means "start with zero brokers configured,"
# the same empty-state landing page a fresh checkout shows.
ENV ONE_MSG_UI_BROKERS_CONFIG=/data/brokers.json

EXPOSE 8010
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8010"]
