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

echo "========== running nutchindex data =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

check_compress

# path check
$HADOOP_EXECUTABLE $RMDIR_CMD $INPUT_HDFS/indexes

# pre-running
SIZE=`dir_size $INPUT_HDFS`
#SIZE=`$HADOOP_EXECUTABLE fs -dus $INPUT_HDFS |  grep -o [0-9]* `
export NUTCH_CONF_DIR=$HADOOP_CONF_DIR:$NUTCH_HOME/conf
START_TIME=`timestamp`

# run bench
echo $NUTCH_HOME
$NUTCH_HOME/bin/nutch index $COMPRESS_OPTS $INPUT_HDFS/indexes $INPUT_HDFS/crawldb $INPUT_HDFS/linkdb $INPUT_HDFS/segments/*

# post-running
END_TIME=`timestamp`
gen_report "NUTCHINDEX" ${START_TIME} ${END_TIME} ${SIZE}
