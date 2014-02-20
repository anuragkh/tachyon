#!/usr/bin/env bash

#start up succinct

bin=`cd "$( dirname "$0" )"; pwd`

$bin/succinct-clients.sh
sleep 5
$bin/succinct-master.sh