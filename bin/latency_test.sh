bin=`cd "$( dirname "$0" )"; pwd`
$bin/perf-test.sh countl $bin/../queries/split_0_queries 100 1000
$bin/perf-test.sh extractl 100 1000
$bin/perf-test.sh locatel $bin/../queries/split_0_queries 100 1