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

echo "========== preparing wordcount data=========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# path check
$HADOOP_EXECUTABLE dfs -rmr $INPUT_HDFS

# generate data
$SPARK_HOME/bin/spark-submit --class RandomTextWriter --master ${SPARK_MASTER} ${DIR}/../data_gen/target/scala-2.10/data-generator_2.10-1.0.jar $INPUT_HDFS ${DATASIZE} ${NUM_PARALLEL}
result=$?
if [ $result -ne 0 ]
then
    echo "ERROR: Spark job failed to run successfully." 
    exit $result
fi
