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

#!/bin/bash

echo "========== running nutchindex data =========="
# configure
DIR=`dirname "$0"`
. ${DIR}/../funcs.sh
configure ${DIR}

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPTS="-D mapred.output.compress=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPTS="-D mapred.output.compress=false"
fi

# path check
$HADOOP_HOME/bin/hadoop fs -rmr $INPUT_HDFS/indexes

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS | awk '{ print $2 }'`
export NUTCH_HOME=$DIR/nutch-1.2
export NUTCH_CONF_DIR=$HADOOP_HOME/conf:$NUTCH_HOME/conf
START_TIME=`timestamp`

# run bench
$NUTCH_HOME/bin/nutch index $COMPRESS_OPTS $INPUT_HDFS/indexes $INPUT_HDFS/crawldb $INPUT_HDFS/linkdb $INPUT_HDFS/segments/*

# post-running
END_TIME=`timestamp`
gen_report "NUTCHINDEX" ${START_TIME} ${END_TIME} ${SIZE} >> ${HIBENCH_REPORT}
