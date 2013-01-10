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

echo "========== preparing hive data =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# compress check
if [ $COMPRESS -eq 1 ]; then
    COMPRESS_OPT="-c ${COMPRESS_CODEC}"
fi

# generate data
OPTION="-t hive \
	-b ${HIVE_BASE_HDFS} \
	-n ${HIVE_INPUT} \
	-m ${NUM_MAPS} \
	-r ${NUM_REDS} \
	-p ${PAGES} \
	-v ${USERVISITS} \
	-o sequence"

START_TIME=`timestamp`

$HADOOP_EXECUTABLE jar ${DATATOOLS} HiBench.DataGen ${OPTION} ${COMPRESS_OPT}

END_TIME=`timestamp`
CODEC=`echo ${COMPRESS_CODEC} | sed 's/.*\.//'`

#gen_report "HIVE_PREPARE ($CODEC)" ${START_TIME} ${END_TIME} "10000" >> prepare.log
