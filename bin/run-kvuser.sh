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

$JAVA -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME $TACHYON_JAVA_OPTS succinctkv.test.User $MASTER_ADDRESS