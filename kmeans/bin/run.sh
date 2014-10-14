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

echo "========== running kmeans bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

MAHOUT_BIN_DIR=$HIBENCH_HOME"/common/hibench/mahout/target"
MAHOUT_RELEASE="mahout-0.9-cdh5.1.0"
if [ $HADOOP_VERSION == "hadoop1" -a $HADOOP_RELEASE != "cdh5" ]; then
  MAHOUT_RELEASE="mahout-distribution-0.7"
fi

if [ ! -e $MAHOUT_BIN_DIR"/"$MAHOUT_RELEASE".tar.gz" ]; then
  echo "Error: The mahout bin file hasn't be downloaded by maven, please check!"
  exit
fi

cd $MAHOUT_BIN_DIR
if [ ! -d $MAHOUT_BIN_DIR"/"$MAHOUT_RELEASE ]; then
  tar zxf $MAHOUT_RELEASE".tar.gz"
fi

MAHOUT_HOME=$MAHOUT_BIN_DIR"/"$MAHOUT_RELEASE

if [ -n "$1" ]; then
  OUTPUT_HDFS=$OUTPUT_HDFS"/"$1
fi

check_compress

$HADOOP_EXECUTABLE $RMDIR_CMD ${OUTPUT_HDFS}

if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
  SSIZE=`grep "BYTES_DATA_GENERATED=" ${DIR}/$TMPLOGFILE | sed 's/BYTES_DATA_GENERATED=//'`
else
  SSIZE=$($HADOOP_EXECUTABLE job -history $INPUT_SAMPLE | grep 'HiBench.Counters.*|BYTES_DATA_GENERATED')
  SSIZE=${SSIZE##*|}
  SSIZE=${SSIZE//,/}
fi

CSIZE=`dir_size $INPUT_CLUSTER`
SIZE=$(($SSIZE+$CSIZE))

# pre-running
OPTION="$COMPRESS_OPT -i ${INPUT_SAMPLE} -c ${INPUT_CLUSTER} -o ${OUTPUT_HDFS} -x ${MAX_ITERATION} -ow -cl -cd 0.5 -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure -xm mapreduce"
START_TIME=`timestamp`

# run bench
${MAHOUT_HOME}/bin/mahout kmeans  ${OPTION}

# post-running
END_TIME=`timestamp`
gen_report "KMEANS" ${START_TIME} ${END_TIME} ${SIZE}

