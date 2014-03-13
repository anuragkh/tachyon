cmd="~/tachyon-succinct/bin/perf-test.sh extractt 1000 $1"
HOSTLIST=~/tachyon-succinct/conf/slaves
for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  echo "Slave: $slave"
	ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no $slave "$cmd" 2>&1 | sed "s/^/$slave: /" &
done
wait
~/ephemeral-hdfs/bin/slaves.sh awk '{ sum += \$1 } END { print sum } ' /root/res_extract_throughput > extract_throughput_$1
~/ephemeral-hdfs/bin/slaves.sh rm /root/res_extract_throughput