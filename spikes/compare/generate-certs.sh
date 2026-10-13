#!/bin/sh
set -eu
umask 077

mkdir -p /certs
if [ -s /certs/ca-cert.pem ] && [ -s /certs/server-cert.pem ] && [ -s /certs/server-key.pem ]; then
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 \
  -keyout /certs/ca-key.pem -out /certs/ca-cert.pem \
  -subj "/CN=Transport Compare CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout /certs/server-key.pem -out /certs/server.csr \
  -subj "/CN=localhost" >/dev/null
cat > /certs/server-ext.cnf <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in /certs/server.csr \
  -CA /certs/ca-cert.pem -CAkey /certs/ca-key.pem -CAcreateserial \
  -out /certs/server-cert.pem -days 365 -sha256 \
  -extfile /certs/server-ext.cnf >/dev/null

rm /certs/ca-key.pem /certs/server.csr /certs/server-ext.cnf /certs/ca-cert.srl
