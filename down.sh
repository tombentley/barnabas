#!/usr/bin/env bash


cat > target/down.conf <<-EOF
foreground=yes
output=/dev/stdout
debug=7
[up]
	accept=localhost:1235
	CAfile=target/cafile.crt
	cert=target/down1.crt
	key=target/down1.key
	client=yes
	connect=localhost:1234
	requireCert=yes
	verify = 2
EOF
cat target/down.conf
stunnel target/down.conf
