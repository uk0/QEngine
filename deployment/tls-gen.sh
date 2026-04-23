#!/usr/bin/env bash
# tls-gen.sh — generate a self-signed CA + server cert for dev / test.
#
# For production use real certificates from your corporate PKI / cert-manager.
# This helper only exists so `docker-compose up` + TLS just works on a fresh
# checkout without external tooling.
#
# Output layout:
#   $OUT_DIR/ca.crt        — CA certificate
#   $OUT_DIR/ca.key        — CA private key (DO NOT ship)
#   $OUT_DIR/server.crt    — server leaf
#   $OUT_DIR/server.key    — server key
#
# Usage:
#   deployment/tls-gen.sh [OUT_DIR]              (default: deployment/tls)
#   SUBJECT_CN=tsdb.local deployment/tls-gen.sh  (override CN for SANs)

set -euo pipefail

OUT_DIR="${1:-$(cd "$(dirname "$0")" && pwd)/tls}"
CN="${SUBJECT_CN:-tsdb.local}"
DAYS="${DAYS:-365}"

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

if [[ -s server.crt ]]; then
  echo "[tls-gen] $OUT_DIR already has a server.crt — skipping (delete to regenerate)"
  exit 0
fi

echo "[tls-gen] writing self-signed CA + leaf into $OUT_DIR (CN=$CN, days=$DAYS)"

# 1. CA
openssl req -x509 -nodes -newkey rsa:2048 -sha256 -days "$DAYS" \
  -keyout ca.key -out ca.crt \
  -subj "/CN=tsdb-dev-ca" >/dev/null 2>&1

# 2. Leaf CSR + signing with SANs for docker-compose service names
cat > san.cnf <<EOF
[req]
distinguished_name = req
prompt = no
[req]
CN = $CN
[v3_req]
subjectAltName = DNS:$CN, DNS:localhost, DNS:qengine-cnode-1, DNS:qengine-cnode-2, DNS:qengine-cnode-3, IP:127.0.0.1
extendedKeyUsage = serverAuth, clientAuth
EOF

openssl req -nodes -newkey rsa:2048 -keyout server.key -out server.csr \
  -subj "/CN=$CN" -config san.cnf >/dev/null 2>&1

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" -sha256 \
  -extfile san.cnf -extensions v3_req >/dev/null 2>&1

rm -f server.csr ca.srl san.cnf
chmod 600 *.key

echo "[tls-gen] done:"
ls -l "$OUT_DIR"
