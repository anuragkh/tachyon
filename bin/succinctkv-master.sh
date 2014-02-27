#!/usr/bin/env bash

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

if [ -z $TACHYON_MASTER_ADDRESS ] ; then
	MASTER_ADDRESS=localhost
fi

echo "Starting SuccinctKVMaster @ `hostname`"
now=$(date +"%Y-%m-%d_%H.%M.%S")
HOSTLIST=$TACHYON_CONF_DIR/slaves
HOSTS=`cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`
for slave in $HOSTS; do
	ARGS="$ARGS $slave"
done
(nohup $JAVA -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="MASTER_LOGGER" -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties $TACHYON_JAVA_OPTS succinctkv.SuccinctKVMaster $ARGS > $TACHYON_LOGS_DIR/succinctkv-master.log@$now 2>&1) &