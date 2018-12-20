#!/usr/bin/env bash

cat > target/up.conf <<-EOF
foreground=yes
;output=/dev/stdout
debug=debug
[up]
	accept=localhost:1234
	CAfile=target/cafile.crt
	cert=target/up1.crt
	key=target/up1.key
	client=no
	connect=localhost:1233
	requireCert=yes
	verify = 2

EOF
cat target/up.conf
stunnel target/up.conf 2>&1 | sed 's/.*/up  : &/'
