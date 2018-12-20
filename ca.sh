#!/bin/bash

set -e
set -x
#CAT > /tmp/ca.conf <<EOF
#[req]
#distinguished_name = req_distinguished_name
#req_extensions = v3_req
#
#[req_distinguished_name]
#
#[v3_req]
#subjectAltName = @alt_names
#
#[alt_names]
#
#EOF

#ca1
openssl req -x509 -new -days 1 \
  -batch -nodes -out target/ca1.crt -keyout target/ca1.key \
  -subj /CN=ca/DN=ca1

# up1
openssl req -new -batch -nodes \
  -keyout target/up1.key -out target/up1.csr \
  -subj /CN=up

openssl x509 -req -days 1 \
  -in target/up1.csr -CA target/ca1.crt -CAkey target/ca1.key \
  -out target/up1.crt

# down1
openssl req -new -batch -nodes \
  -keyout target/down1.key -out target/down1.csr \
  -subj /CN=down

openssl x509 -req -days 1 \
  -in target/down1.csr -CA target/ca1.crt -CAkey target/ca1.key \
  -out target/down1.crt


# ca2
openssl req -x509 -new -days 10 \
  -batch -nodes -out target/ca2.crt -keyout target/ca2.key \
  -subj /CN=ca/DN=ca2

# up2
openssl req -new -batch -nodes \
  -keyout target/up2.key -out target/up2.csr \
  -subj /CN=up

openssl x509 -req -days 1 \
  -in target/up2.csr -CA target/ca2.crt -CAkey target/ca2.key \
  -out target/up2.crt


# down2
openssl req -new -batch -nodes \
  -keyout target/down2.key -out target/down2.csr \
  -subj /CN=down


openssl x509 -req -days 10 \
  -in target/down2.csr -CA target/ca2.crt -CAkey target/ca2.key \
  -out target/down2.crt

#cat target/up.crt target/ca.crt > target/stunnel-up.crt
cat target/ca1.crt target/ca2.crt > target/cafile.crt


