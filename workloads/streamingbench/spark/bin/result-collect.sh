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
workload_root=${workload_folder}/../..
log_dir="report/streamingbench/spark"
log_dir=$workload_root/../../$log_dir

# use regular expression to match sentences
platform="(Spark|Storm|Samza|Gearpump|Flink])-(.*)"
metric="(Throughput|Average Latency|Latency|Config)"
pattern="^"$platform": "$metric": (.*)$"

if [ ! -d $log_dir ]; then
  echo "Directory not found"
  exit 1
fi

# echo the header of the table
echo "Platform-Workload, Throughput(records/s), AvgLatency(ms), Configuration, BatchLatency(ms), BatchRecordCount(records)"

last_workload=""
config=""
latency_list=""

while read line
do
  if [[ $line =~ $pattern ]]; then
    workload=${BASH_REMATCH[1]}"-"${BASH_REMATCH[2]}
    type=${BASH_REMATCH[3]}
    msg=${BASH_REMATCH[4]}

    # when workload changes, echo the line of last workload
    if [ "$workload" != "$last_workload" ]; then
      if [ "$last_workload" != "" ]; then
        out_line=$last_workload", "$throughput", "$avg_latency", "$config$latency_list
        throughput=""
        avg_latency=""
        config=""
        latency_list=""
        echo $out_line
      fi
      last_workload=$workload
    fi

    # different parse logic for different workload types
    case $type in
      "Throughput") throughput=`echo $msg | awk '{print $1}'` ;;
      "Average Latency") avg_latency=`echo $msg | awk '{print $1}'` ;;
      "Config")
        if [ "$config" != "" ]; then
          # using semicolon to seperate multiple configurations
          config=$config"; "`echo $msg | awk '{print $1}'`
        else
          config=`echo $msg | awk '{print $1}'`
        fi ;;
      "Latency") latency_list=$latency_list`echo $msg | awk '{split($0, a, " "); print ", "a[1]", "a[4]}'`;;
      *) ;;
    esac
  fi
# input all files inside the log directory except bench.log
done < <(find $log_dir -type f ! -name bench.log | xargs cat)

# echo the line of the workload in the end
if [ "$last_workload" != "" ]; then
  out_line=$last_workload", "$throughput", "$avg_latency", "$config$latency_list
  echo $out_line
fi
