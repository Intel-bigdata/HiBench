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

#This script takes t argument scaleFactor

workload_folder=`dirname "$0"`
workload_folder=`cd "$workload_folder"; pwd`
workload_root=${workload_folder}/..
. "${workload_root}/../../bin/functions/load-bench-config.sh"

# generate seed data1 by hive
enter_bench HadoopPrepareSeed ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $STREAMING_DATA_DIR || true
echo -e "${On_Blue}Pages:${PAGES}, USERVISITS:${USERVISITS}${Color_Off}"

OPTION="-t hive \
        -b ${STREAMING_DATA_DIR} \
        -n ${STREAMING_DATA1_NAME} \
        -m ${NUM_MAPS} \
        -r ${NUM_REDS} \
        -p ${PAGES} \
        -v ${USERVISITS}"

START_TIME=`timestamp`
run-hadoop-job ${DATATOOLS} HiBench.DataGen ${OPTION} ${DATATOOLS_COMPRESS_OPT}
END_TIME=`timestamp`
SIZE="0"

show_bannar finish
leave_bench

# generate seed data2 by kmeans
enter_bench HadoopPrepareSeed2 ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $STREAMING_DATA2_SAMPLE_DIR || true
OPTION="-sampleDir ${STREAMING_DATA2_SAMPLE_DIR} -clusterDir ${STREAMING_DATA2_CLUSTER_DIR} -numClusters ${NUM_OF_CLUSTERS} -numSamples ${NUM_OF_SAMPLES} -samplesPerFile ${SAMPLES_PER_INPUTFILE} -sampleDimension ${DIMENSIONS} -textOutput"
export HADOOP_CLASSPATH=`${MAHOUT_HOME}/bin/mahout classpath`
export_withlog HADOOP_CLASSPATH
run-hadoop-job ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset -D hadoop.job.history.user.location=${STREAMING_DATA2_SAMPLE_DIR} ${OPTION} 
END_TIME=`timestamp`

show_bannar finish
leave_bench

# Copy and convert as seed
LocalDir=${WORKLOAD_RESULT_FOLDER}/Seed
rm -rf $LocalDir 2> /dev/null
echo "Copying uservists table from HDFS to local disk: ${LocalDir}"
CMD="$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR fs -copyToLocal ${STREAMING_DATA1_DIR}/uservisits ${LocalDir}"
execute_withlog ${CMD}
cat $LocalDir/part-* | awk '{split($2,a,",");  print $1, a[1], a[3], "00:00:00", a[4], a[2];}' | awk "{print substr(\$0, 1, $ll{STREAMING_DATA1_LENGTH});}" > ${LocalDir}/seed.data

# Generate data by seed
DATA_GEN_DIR=${workload_root}/../../src/streambench/datagen
text_dataset="${DATA_GEN_DIR}/src/main/resources/test1.data"
text_dataset2="${DATA_GEN_DIR}/src/main/resources/test2.data"
text_seed=${LocalDir}/seed.data

if [ ! -f $text_seed ]; then
	echo "Usage: Require $text_seed file. Please re-download or generate it first."
	exit 1
fi

seed_line_length=`head -2 $text_seed | tail -1 | wc -c`
echo "Seed first record size:$seed_line_length"

scale_factor=${STREAMING_DATA_SCALE_FACTOR}

echo "========== start gen dataset1 with scale factor:$scale_factor ============="
awk_script="{"
for i in `seq $scale_factor`; do
    awk_script=$awk_script+"print $0;"
done
awk_script=$awk_script+"}"
cat $text_seed | awk '$awk_script' > $text_dataset

# Generate data2
echo "========== start gen dataset2 with scale factor:$scale_factor ============="
$HADOOP_EXECUTABLE --config $HADOOP_CONF_DIR fs -cat ${STREAMING_DATA2_SAMPLE_DIR}/part-\* | ${workload_folder}/parse_data2.py ${scale_factor} > $text_dataset2

echo "========== dataset generated ============="


