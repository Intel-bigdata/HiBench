#!/bin/sh

set -u
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
DIR=`cd $bin/../; pwd`
. "${DIR}/conf/configure.sh"
echo "=========start storm benchmark $benchName========="

benchArgs="$nimbus $nimbusAPIPort $zkHost $workerCount $spoutThreads $benchName $recordCount $topic $consumer"
if [ "$benchName" == "micro-sample"  ]; then
  benchArgs="$benchArgs $prob"
elif [ "$benchName" == "micro-grep"  ]; then
  benchArgs="$benchArgs $pattern"
else
  benchArgs="$benchArgs $separator $fieldIndex "
fi

echo "Args:$benchArgs"

cd ${DIR}

$STORM_BIN_HOME/storm jar ${DIR}/target/streaming-bench-storm-0.1-SNAPSHOT-jar-with-dependencies.jar com.intel.PRCcloud.RunBench $benchArgs
