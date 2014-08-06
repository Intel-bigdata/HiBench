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
INPUT_HDFS=${DATA_HDFS}/KMeans/Input
OUTPUT_HDFS=${DATA_HDFS}/KMeans/Output
if [ $COMPRESS -eq 1 ]; then
    INPUT_HDFS=${INPUT_HDFS}-comp
    OUTPUT_HDFS=${OUTPUT_HDFS}-comp
fi
INPUT_SAMPLE=${INPUT_HDFS}/samples
INPUT_CLUSTER=${INPUT_HDFS}/cluster

# for prepare
NUM_OF_CLUSTERS=5
#NUM_OF_SAMPLES=20000000
NUM_OF_SAMPLES=3000000
#SAMPLES_PER_INPUTFILE=4000000
SAMPLES_PER_INPUTFILE=600000
DIMENSIONS=20

# for running
MAX_ITERATION=5
K=10
CONVERGEDIST=0.5
