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
set -u

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== running python join bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../../bin/sparkbench-config.sh"
. "${DIR}/../conf/configure.sh"

# compress
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-D mapred.output.compress=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-D mapred.output.compress=false"
fi

# path check
$HADOOP_EXECUTABLE dfs -rmr  $OUTPUT_HDFS

# pre-running
SIZE=`dir_size $INPUT_HDFS` 
START_TIME=`timestamp`

# run bench
$SPARK_HOME/bin/spark-submit --master ${SPARK_MASTER} ${SPARKBENCH_HOME}/common/src/main/python//join.py $INPUT_HDFS $OUTPUT_HDFS

# post-running
END_TIME=`timestamp`
gen_report "PythonJoin" ${START_TIME} ${END_TIME} ${SIZE}
