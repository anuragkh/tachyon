cmd="~/tachyon-succinct/bin/perf-test.sh countt /root/tachyon-succinct/queries/split_0_queries 1000 $1"
HOSTLIST=~/tachyon-succinct/conf/slaves
for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  echo "Slave: $slave"
	ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no $slave "$cmd" 2>&1 | sed "s/^/$slave: /" &
done
wait
~/ephemeral-hdfs/bin/slaves.sh awk '{ sum += \$1 } END { print sum } ' /root/res_count_throughput > count_throughput_$1
~/ephemeral-hdfs/bin/slaves.sh rm /root/res_count_throughput