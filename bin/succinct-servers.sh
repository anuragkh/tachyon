#!/usr/bin/env bash

bin=`cd "$( dirname "$0" )"; pwd`
DEFAULT_LIBEXEC_DIR="$bin"/../libexec
TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $TACHYON_LIBEXEC_DIR/tachyon-config.sh
TACHYON_TARGET_DIR="$bin"/../target
TACHYON_LOG_PATH="$bin"/../logs

HOSTLIST=$TACHYON_CONF_DIR/slaves
for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
	cmd="$bin/succinct-server.sh"
  	ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no $slave "$cmd" 2>&1 | sed "s/^/$slave: /" &
 	sleep 0.02
done

wait
