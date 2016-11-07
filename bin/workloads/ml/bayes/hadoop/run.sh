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

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../../
workload_config=${root_dir}/conf/workloads/ml/bayes.conf
. "${root_dir}/bin/functions/load-bench-config.sh"

enter_bench HadoopBayes ${workload_config} ${current_dir}
show_bannar start

ensure-mahout-release
rmr-hdfs $OUTPUT_HDFS || true

SIZE=`dir_size $INPUT_HDFS`

START_TIME=`timestamp`

# strip spaces after "-D"
COMPRESS_OPT=-Dmapreduce.output.fileoutputformat.compress=false
MONITOR_PID=`start-monitor`
CMD1="$MAHOUT_HOME/bin/mahout seq2sparse ${COMPRESS_OPT} -i ${INPUT_HDFS} -o ${OUTPUT_HDFS}/vectors  -lnorm -nv  -wt tfidf -ng ${NGRAMS} --numReducers $NUM_REDS"
execute_withlog ${CMD1}

if [ ${HADOOP_RELEASE} == "cdh5" ]
then
   CMD2="$MAHOUT_HOME/bin/mahout trainnb ${COMPRESS_OPT} -i ${OUTPUT_HDFS}/vectors/tfidf-vectors -el -o ${OUTPUT_HDFS}/model -li ${OUTPUT_HDFS}/labelindex  -ow --tempDir ${OUTPUT_HDFS}/temp"
elif [ ${HADOOP_RELEASE} == "apache" ]
then
   CMD2="$MAHOUT_HOME/bin/mahout trainnb ${COMPRESS_OPT} -i ${OUTPUT_HDFS}/vectors/tfidf-vectors -o ${OUTPUT_HDFS}/model -li ${OUTPUT_HDFS}/labelindex  -ow --tempDir ${OUTPUT_HDFS}/temp"
elif [ ${HADOOP_RELEASE} == "hdp" ]
then
   CMD2="$MAHOUT_HOME/bin/mahout trainnb ${COMPRESS_OPT} -i ${OUTPUT_HDFS}/vectors/tfidf-vectors -o ${OUTPUT_HDFS}/model -li ${OUTPUT_HDFS}/labelindex  -ow --tempDir ${OUTPUT_HDFS}/temp"
fi
execute_withlog ${CMD2}

END_TIME=`timestamp`
stop-monitor $MONITOR_PID

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench
