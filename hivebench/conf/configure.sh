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
HIVE_INPUT="Input"
HIVE_OUTPUT="Output"
HIVE_BASE_HDFS=${DATA_HDFS}/Hive

if [ $COMPRESS -eq 1 ]; then
    HIVE_INPUT=${HIVE_INPUT}-comp
    HIVE_OUTPUT=${HIVE_OUTPUT}-comp
fi

INPUT_HDFS=${HIVE_BASE_HDFS}/${HIVE_INPUT}
OUTPUT_HDFS=${HIVE_BASE_HDFS}/${HIVE_OUTPUT}

# for prepare (in total) 1G-rankings (text), 20G-uservisits (text)
USERVISITS=100000000
PAGES=12000000

# for prepare & running (in total)
NUM_MAPS=96
NUM_REDS=48
