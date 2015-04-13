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

workload_folder=`dirname "$0"`
workload_folder=`cd "$workload_folder"; pwd`
workload_root=${workload_folder}/..
. "${workload_root}/../../bin/functions/load-bench-config.sh"

enter_bench HadoopPrepareBayes ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs ${INPUT_HDFS} || true
START_TIME=`timestamp`
OPTION="-t bayes \
        -b ${BAYES_BASE_HDFS} \
        -n ${BAYES_INPUT} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -class ${CLASSES} \
        -o sequence"

run-hadoop-job ${DATATOOLS} HiBench.DataGen ${OPTION} ${DATATOOLS_COMPRESS_OPT}
END_TIME=`timestamp`

show_bannar finish
leave_bench
