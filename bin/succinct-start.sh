#!/usr/bin/env bash

#start up succinct

bin=`cd "$( dirname "$0" )"; pwd`

$bin/succinct-workers.sh
sleep 5
$bin/succinct-master.sh
