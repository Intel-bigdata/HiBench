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

echo "========== preparing sort data=========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

check_compress
$HADOOP_EXECUTABLE $RMDIR_CMD $INPUT_HDFS

if [ "x"$HADOOP_VERSION == "xhadoop1" ]; then
#--- for hadoop version 1 ---

  # generate data
  $HADOOP_EXECUTABLE jar $HADOOP_EXAMPLES_JAR randomtextwriter \
    -D test.randomtextwrite.bytes_per_map=$((${DATASIZE} / ${NUM_MAPS})) \
    -D test.randomtextwrite.maps_per_host=${NUM_MAPS} \
    $COMPRESS_OPT \
    $INPUT_HDFS

else
#--- for hadoop version 2.0.5 above ---

  # generate data
  $HADOOP_EXECUTABLE jar $HADOOP_EXAMPLES_JAR randomtextwriter \
    -D mapreduce.randomtextwriter.bytespermap=$((${DATASIZE} / ${NUM_MAPS})) \
    -D mapreduce.randomtextwriter.mapsperhost=${NUM_MAPS} \
    $COMPRESS_OPT \
    $INPUT_HDFS

fi
