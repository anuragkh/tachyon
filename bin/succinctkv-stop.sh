#!/usr/bin/env bash

usage="Usage: succinctkv-stop.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

$bin/tachyon killAll succinctkv.SuccinctKVMaster
$bin/tachyon killAll succinctkv.SuccinctKVWorker

$bin/tachyon-slaves.sh $bin/tachyon killAll succinctkv.SuccinctKVWorker
