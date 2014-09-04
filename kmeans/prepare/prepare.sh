#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== preparing kmeans data =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"


# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-compress true \
        -compressCodec $COMPRESS_CODEC \
        -compressType BLOCK "
else
    COMPRESS_OPT="-compress false"
fi

# paths check
trap '$HADOOP_EXECUTABLE dfs -rmr ${INPUT_HDFS_DIR}' EXIT

# generate data
OPTION="-sampleDir ${INPUT_SAMPLE} -clusterDir ${INPUT_CLUSTER} -numClusters ${NUM_OF_CLUSTERS} -numSamples ${NUM_OF_SAMPLES} -samplesPerFile ${SAMPLES_PER_INPUTFILE} -sampleDimension ${DIMENSIONS}"
export HADOOP_CLASSPATH=`${MAHOUT_HOME}/bin/mahout classpath | tail -1`

"$HADOOP_EXECUTABLE" --config $HADOOP_CONF_DIR jar $MAHOUT_HOME/examples/target/mahout-examples-0.7.jar org.apache.mahout.clustering.kmeans.GenKMeansDataset -libjars $MAHOUT_HOME/core/target/mahout-core-0.7.jar,$MAHOUT_HOME/examples/target/mahout-examples-0.7-job.jar -D hadoop.job.history.user.location=${INPUT_SAMPLE} ${COMPRESS_OPT} ${OPTION}
result=$?
if [ $result -ne 0 ]
then
    echo "ERROR: Hadoop job failed to run successfully." 
    exit $result
fi
( cd `dirname $0` && sbt "run ${HDFS_MASTER} ${INPUT_SAMPLE} ${INPUT_HDFS}" )
result=$?
if [ $result -ne 0 ]
then
    echo "ERROR: covert job failed to run successfully." 
    exit $result
fi

