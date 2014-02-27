#!/usr/bin/env bash

usage="Usage: succinct-stop.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

$bin/tachyon killAll succinct.SuccinctMaster
$bin/tachyon killAll succinct.SuccinctWorker

$bin/tachyon-slaves.sh $bin/tachyon killAll succinct.SuccinctWorker
