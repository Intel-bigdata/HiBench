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

echo "========== preparing bayes data =========="
# configure
DIR=`dirname "$0"`
. ${DIR}/../funcs.sh
configure ${DIR}

# compress check
if [ ${COMPRESS} -eq 1 ]; then
    COMPRESS_OPT="-codec ${COMPRESS_CODEC}"
else
    COMPRESS_OPT=-nocompress
fi

# path check
if [ ! -d ${INPUT_LOCAL} ]; then
    echo "input dir ${INPUT_LOCAL} not exist!"
    exit 1
fi
${HADOOP_HOME}/bin/hadoop fs -rmr ${INPUT_HDFS}
${HADOOP_HOME}/bin/hadoop fs -rmr ${INPUT_HDFS}-tmp

# generate data
echo "copy local data ${INPUT_LOCAL} to hdfs......"
${HADOOP_HOME}/bin/hadoop fs -put ${INPUT_LOCAL} ${INPUT_HDFS}-tmp

$HADOOP_HOME/bin/hadoop jar ${HIBENCH_HOME}/common/compression/dist/compression.jar ${COMPRESS_OPT} ${INPUT_HDFS}-tmp ${INPUT_HDFS}

${HADOOP_HOME}/bin/hadoop fs -rmr ${INPUT_HDFS}-tmp
$HADOOP_HOME/bin/hadoop fs -rmr ${INPUT_HDFS}/_*

