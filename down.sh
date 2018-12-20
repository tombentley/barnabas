#!/usr/bin/env bash


cat > target/down.conf <<-EOF
foreground=yes
;output=/dev/stdout
debug=debug
[down]
	accept=localhost:1235
	CAfile=target/cafile.crt
	cert=target/down2.crt
	key=target/down2.key
	client=yes
	connect=localhost:1234
	requireCert=yes
	verify = 2

EOF
cat target/down.conf
stunnel target/down.conf 2>&1 | sed 's/.*/down: &/'
