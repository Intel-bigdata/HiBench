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

# compress
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# paths
INPUT_HDFS_DIR=${DATA_HDFS}/KMeans/Input
OUTPUT_HDFS=${DATA_HDFS}/KMeans/Output
if [ $COMPRESS -eq 1 ]; then
    INPUT_HDFS_DIR=${INPUT_HDFS_DIR}-comp
    OUTPUT_HDFS=${OUTPUT_HDFS}-comp
fi
INPUT_SAMPLE=${INPUT_HDFS_DIR}/samples
INPUT_CLUSTER=${INPUT_HDFS_DIR}/cluster
INPUT_HDFS=${INPUT_SAMPLE}.txt

# for prepare
NUM_OF_CLUSTERS=5
DIMENSIONS=20
#NUM_OF_SAMPLES=20000000
#SAMPLES_PER_INPUTFILE=4000000

#small scale
NUM_OF_SAMPLES=3000000
SAMPLES_PER_INPUTFILE=600000

# for running
MAX_ITERATION=5
K=10
CONVERGEDIST=0.5
