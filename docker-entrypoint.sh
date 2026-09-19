#!/bin/sh
# Serves HTTPS for local/internal use — every user runs this on their own
# machine (docker run ... on localhost), so there's no shared public domain
# a real CA (Let's Encrypt or otherwise) could issue a trusted cert for.
# A self-signed cert is the honest option here: browsers show a one-time
# "not secure" warning to click through, same as any local HTTPS dev setup
# (create-react-app, mkcert, etc.) — not a workaround, just what "HTTPS with
# no public domain" actually looks like.
#
# Generated fresh per container by default. If /data is mounted (see
# README.md's Packaging section — the same volume that persists
# brokers.json), the cert is generated once and reused from there, so a
# restart doesn't hand every user a brand-new cert (and a fresh browser
# warning) every single time.
set -eu

CERT_DIR="${ONE_MSG_UI_TLS_DIR:-/data/tls}"
CERT_FILE="$CERT_DIR/cert.pem"
KEY_FILE="$CERT_DIR/key.pem"

if [ ! -f "$CERT_FILE" ] || [ ! -f "$KEY_FILE" ]; then
  mkdir -p "$CERT_DIR"
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$KEY_FILE" -out "$CERT_FILE" \
    -days 825 -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    2>/dev/null
  echo "Generated a new self-signed TLS certificate at $CERT_DIR (valid for localhost/127.0.0.1)."
fi

exec uvicorn api.main:app --host 0.0.0.0 --port 8010 --ssl-keyfile "$KEY_FILE" --ssl-certfile "$CERT_FILE"
