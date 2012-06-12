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

#!/bin/sh

echo "========== running pagerank bench =========="
# configure
DIR=`dirname "$0"`
source ${DIR}/../funcs.sh
configure ${DIR}

# compress check
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-Dmapred.output.compress=true \
    -Dmapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-Dmapred.output.compress=false"
fi

# path check
$HADOOP_HOME/bin/hadoop dfs -rmr $TEMP_HDFS
$HADOOP_HOME/bin/hadoop dfs -rmr $OUTPUT_HDFS

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS | awk '{ print $2 }'`
OPTION="${COMPRESS_OPT} --vertices ${INPUT_HDFS}/vertices --edges ${INPUT_HDFS}/edges --output ${OUTPUT_HDFS} --numIterations ${NUM_ITERATIONS} --tempDir ${TEMP_HDFS}"
START_TIME=`timestamp`

# run bench
$MAHOUT_HOME/bin/mahout pagerank $OPTION

# post-running
END_TIME=`timestamp`
gen_report "PAGERANK" ${START_TIME} ${END_TIME} ${SIZE} >> ${HIBENCH_REPORT}
