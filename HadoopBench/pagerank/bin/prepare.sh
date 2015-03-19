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

echo "========== preparing pagerank data =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# compress
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-c ${COMPRESS_CODEC}"
fi

rm -rf ${DIR}/$TMPLOGFILE
# generate data
#DELIMITER=\t
OPTION="-t pagerank \
	-b ${PAGERANK_BASE_HDFS} \
	-n ${PAGERANK_INPUT} \
	-m ${NUM_MAPS} \
	-r ${NUM_REDS} \
	-p ${PAGES} \
        -pbalance -pbalance \
	-o text"

#	-d ${DELIMITER} \
$HADOOP_EXECUTABLE jar ${DATATOOLS} HiBench.DataGen ${OPTION} ${COMPRESS_OPT} 2>&1 | tee ${DIR}/$TMPLOGFILE

