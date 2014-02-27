#!/usr/bin/env bash

#start up succinct

bin=`cd "$( dirname "$0" )"; pwd`

$bin/succinctkv-workers.sh
sleep 5
$bin/succinctkv-master.sh
