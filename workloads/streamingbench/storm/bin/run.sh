#!/bin/sh

set -u
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
DIR=`cd $bin/../; pwd`
SRC_DIR="$DIR/../../../src/streambench/stormbench"

. "${SRC_DIR}/conf/configure.sh"
echo "=========start storm benchmark $benchName========="

benchArgs="$nimbus $nimbusAPIPort $zkHost $workerCount $spoutThreads $boltThreads $benchName $recordCount $topic $consumer $readFromStart $ackon $nimbusContactInterval"
if [ "$benchName" == "micro-sample"  ]; then
  benchArgs="$benchArgs $prob"
elif [ "$benchName" == "trident-sample"  ]; then
  benchArgs="$benchArgs $prob"
elif [ "$benchName" == "micro-grep"  ]; then
  benchArgs="$benchArgs $pattern"
elif [ "$benchName" == "trident-grep"  ]; then
  benchArgs="$benchArgs $pattern"
else
  benchArgs="$benchArgs $separator $fieldIndex "
fi

echo "Args:$benchArgs"

cd ${DIR}

$STORM_BIN_HOME/storm jar ${SRC_DIR}/target/streaming-bench-storm-0.1-SNAPSHOT-jar-with-dependencies.jar com.intel.PRCcloud.RunBench $benchArgs
