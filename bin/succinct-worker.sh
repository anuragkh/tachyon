#!/usr/bin/env bash

bin=`cd "$( dirname "$0" )"; pwd`

ensure_dirs() {
  	if [ ! -d "$TACHYON_LOGS_DIR" ]; then
    	echo "TACHYON_LOGS_DIR: $TACHYON_LOGS_DIR"
    	mkdir -p $TACHYON_LOGS_DIR
  	fi
}

get_env() {
  	DEFAULT_LIBEXEC_DIR="$bin"/../libexec
  	TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  	. $TACHYON_LIBEXEC_DIR/tachyon-config.sh
}

# get environment
get_env

# ensure log/data dirs
ensure_dirs

MASTER_ADDRESS=$TACHYON_MASTER_ADDRESS
if [ -z $TACHYON_MASTER_ADDRESS ] ; then
	MASTER_ADDRESS=localhost
fi
now=$(date +"%Y-%m-%d_%H.%M.%S")
HOSTLIST=$TACHYON_CONF_DIR/slaves
HOSTS=`cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`
for slave in $HOSTS; do
	ARGS="$ARGS $slave"
done
echo "Starting SuccinctWorker @ `hostname`"
(nohup $JAVA -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="WORKER_LOGGER" -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties $TACHYON_JAVA_OPTS succinct.SuccinctWorker $MASTER_ADDRESS > $TACHYON_LOGS_DIR/succinct-worker.log@$now 2>&1) &