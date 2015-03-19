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
set -u 

# compress
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# paths
PAGERANK_BASE_HDFS=${DATA_HDFS}/Pagerank
PAGERANK_INPUT="Input"
PAGERANK_OUTPUT="Output"

if [ $COMPRESS -eq 1 ]; then
    PAGERANK_INPUT=${PAGERANK_INPUT}-comp
    PAGERANK_OUTPUT=${PAGERANK_OUTPUT}-comp
fi

INPUT_HDFS_DIR=${PAGERANK_BASE_HDFS}/${PAGERANK_INPUT}
OUTPUT_HDFS_DIR=${PAGERANK_BASE_HDFS}/${PAGERANK_OUTPUT}

INPUT_HDFS=${INPUT_HDFS_DIR}/edges.txt
OUTPUT_HDFS=${OUTPUT_HDFS_DIR}.txt
	
# for preparation (per node) - 32G
#PAGES=500000
PAGES=5000      # small scale

# for running (in total)
NUM_ITERATIONS=3
BLOCK=0
BLOCK_WIDTH=16
