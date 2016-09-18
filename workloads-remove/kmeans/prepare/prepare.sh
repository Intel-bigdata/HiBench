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

enter_bench HadoopPrepareKmeans ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $INPUT_HDFS || true
ensure-mahout-release

START_TIME=`timestamp`

OPTION="-sampleDir ${INPUT_SAMPLE} -clusterDir ${INPUT_CLUSTER} -numClusters ${NUM_OF_CLUSTERS} -numSamples ${NUM_OF_SAMPLES} -samplesPerFile ${SAMPLES_PER_INPUTFILE} -sampleDimension ${DIMENSIONS}"
export HADOOP_CLASSPATH=`${MAHOUT_HOME}/bin/mahout classpath`
export_withlog HADOOP_CLASSPATH
#run-hadoop-job ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset -libjars $MAHOUT_HOME/mahout-core-0.7-job.jar,$MAHOUT_HOME/mahout-examples-0.7-job.jar -D hadoop.job.history.user.location=${INPUT_SAMPLE} ${OPTION} 
run-hadoop-job ${DATATOOLS} org.apache.mahout.clustering.kmeans.GenKMeansDataset -D hadoop.job.history.user.location=${INPUT_SAMPLE} ${KMEANS_COMPRESS_OPT} ${OPTION} 
END_TIME=`timestamp`

show_bannar finish
leave_bench


#run-spark-job --jars $MAHOUT_HOME/mahout-core-0.7.jar,$MAHOUT_HOME/mahout-examples-0.7-job.jar com.intel.sparkbench.datagen.convert.KmeansConvert ${INPUT_SAMPLE} ${INPUT_HDFS}
#${SPARK_HOME}/bin/spark-submit --jars $MAHOUT_HOME/core/target/mahout-core-0.7.jar,$MAHOUT_HOME/examples/target/mahout-examples-0.7-job.jar --class com.intel.sparkbench.datagen.convert.KmeansConvert --master ${SPARK_MASTER} ${SPARKBENCH_JAR} ${INPUT_SAMPLE} ${INPUT_HDFS}

