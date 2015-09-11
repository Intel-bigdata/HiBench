#!/bin/bash

set -u
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
DIR=`cd $bin/../; pwd`
. "${DIR}/conf/configure.sh"
echo "=========stop storm benchmark $benchName========="

cd ${DIR}
$STORM_BIN_HOME/storm kill $benchName
