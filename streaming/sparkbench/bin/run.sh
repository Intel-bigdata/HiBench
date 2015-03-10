#!/bin/bash

set -u
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
DIR=`cd $bin/../; pwd`
. "${DIR}/conf/configure.sh"
echo "=========start spark benchmark $benchName========="

benchArgs="$benchName $topicName $sparkMaster $batchInterval $zkHost $consumerGroup $receiverNodes $recordCount $copies $debug"
if [ "$benchName" == "micro/wordcount" ]; then
  benchArgs="$benchArgs $separator"
elif [ "$benchName" == "micro/sample"  ]; then
  benchArgs="$benchArgs $prob"
elif [ "$benchName" == "micro/grep"  ]; then
  benchArgs="$benchArgs $pattern"
else
  benchArgs="$benchArgs $fieldIndex $separator"
fi

echo "Args:$benchArgs"

$SPARK_BIN_DIR/spark-submit --class  com.intel.PRCcloud.streamBench.RunBench ${DIR}/target/scala-2.10/streaming-bench-spark_0.1-assembly-1.0.0.jar $benchArgs 2>&1 | tee consoleLog.txt
