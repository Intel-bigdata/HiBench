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

# paths
INPUT_HDFS=${DATA_HDFS}/TeraSort/Input
OUTPUT_HDFS=${DATA_HDFS}/TeraSort/Output

# for preparation (per node) - 32G
#DATASIZE=32000000000   # 320M records, 100Bytes each = 3.2TB
DATASIZE=320000000      # small scale, 320M records * 100 = 32GB
#DATASIZE=3200000        # tiny scale, 3.2M records * 100 = 320MB

# for genreport
SIZE=$(( $DATASIZE * 100 ))  
