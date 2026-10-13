#!/bin/sh
set -eu
umask 077

mkdir -p /auth/certs
if [ -s /auth/certs/ca-cert.pem ] && [ -s /auth/certs/server-cert.pem ] \
  && [ -s /auth/certs/server-key.pem ] && [ -s /auth/certs/client-cert.pem ] \
  && [ -s /auth/certs/client-key.pem ]; then
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 \
  -keyout /auth/certs/ca-key.pem \
  -out /auth/certs/ca-cert.pem \
  -subj "/CN=NATS Auth Spike CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout /auth/certs/server-key.pem \
  -out /auth/certs/server.csr \
  -subj "/CN=localhost" >/dev/null
cat > /auth/certs/server-ext.cnf <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in /auth/certs/server.csr \
  -CA /auth/certs/ca-cert.pem -CAkey /auth/certs/ca-key.pem \
  -CAcreateserial -out /auth/certs/server-cert.pem -days 365 -sha256 \
  -extfile /auth/certs/server-ext.cnf >/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout /auth/certs/client-key.pem \
  -out /auth/certs/client.csr \
  -subj "/O=Dunamis/CN=relay-spike" >/dev/null
cat > /auth/certs/client-ext.cnf <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in /auth/certs/client.csr \
  -CA /auth/certs/ca-cert.pem -CAkey /auth/certs/ca-key.pem \
  -CAcreateserial -out /auth/certs/client-cert.pem -days 365 -sha256 \
  -extfile /auth/certs/client-ext.cnf >/dev/null

rm /auth/certs/ca-key.pem /auth/certs/server.csr /auth/certs/server-ext.cnf \
  /auth/certs/client.csr /auth/certs/client-ext.cnf /auth/certs/ca-cert.srl
