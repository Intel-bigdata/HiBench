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
. "${DIR}/../bin/load-sparkbench-config.sh"
. "${DIR}/conf/configure.sh"

# prepare for mahout
if [ ! -e $DEPENDENCY_DIR"/mahout/target/"$MAHOUT_RELEASE".tar.gz" ]; then
  echo "Error: The mahout bin file hasn't be downloaded by maven, please check!"
  exit
fi

cd $DEPENDENCY_DIR"/mahout/target/"
if [ ! -d $DEPENDENCY_DIR"/mahout/target/"$MAHOUT_RELEASE ]; then
  tar zxf $MAHOUT_RELEASE".tar.gz"
fi

MAHOUT_HOME=$DEPENDENCY_DIR"/mahout/target/"$MAHOUT_RELEASE

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-compress true \
        -compressCodec $COMPRESS_CODEC \
        -compressType BLOCK "
else
    COMPRESS_OPT="-compress false"
fi

# paths check
$HADOOP_EXECUTABLE dfs -rmr ${INPUT_HDFS_DIR} || true

# generate data
OPTION="-sampleDir ${INPUT_SAMPLE} -clusterDir ${INPUT_CLUSTER} -numClusters ${NUM_OF_CLUSTERS} -numSamples ${NUM_OF_SAMPLES} -samplesPerFile ${SAMPLES_PER_INPUTFILE} -sampleDimension ${DIMENSIONS}"
export HADOOP_CLASSPATH=`${MAHOUT_HOME}/bin/mahout classpath`
"$HADOOP_EXECUTABLE" --config $HADOOP_CONF_DIR jar ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset -libjars $MAHOUT_HOME/mahout-core-0.7-job.jar,$MAHOUT_HOME/mahout-examples-0.7-job.jar -D hadoop.job.history.user.location=${INPUT_SAMPLE} ${COMPRESS_OPT} ${OPTION} 
result=$?
if [ $result -ne 0 ]
then
    echo "ERROR: Hadoop job failed to run successfully." 
    exit $result
fi
run-spark-job --jars $MAHOUT_HOME/mahout-core-0.7.jar,$MAHOUT_HOME/mahout-examples-0.7-job.jar com.intel.sparkbench.datagen.convert.KmeansConvert ${INPUT_SAMPLE} ${INPUT_HDFS}
#${SPARK_HOME}/bin/spark-submit --jars $MAHOUT_HOME/core/target/mahout-core-0.7.jar,$MAHOUT_HOME/examples/target/mahout-examples-0.7-job.jar --class com.intel.sparkbench.datagen.convert.KmeansConvert --master ${SPARK_MASTER} ${SPARKBENCH_JAR} ${INPUT_SAMPLE} ${INPUT_HDFS}

