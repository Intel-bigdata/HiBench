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
log_file="report/streamingbench/spark/streambenchlog.txt"
log_file=$workload_root/../../$log_file

if [ ! -f $log_file ]; then
  echo "Input file not found"
  exit 1
fi

# Platform-Workload Throughput avgLatency config latency...
last_workload=""
throughput=""
avg_latency=""
config=""
latency_list=""

while read line
do
  workload=`echo $line | awk '{split($0, a, ": "); print a[1]}'`
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
  type=`echo $line | awk '{split($0, a, ": "); print a[2]}'`
  if [ "$type" == "Message" ]; then
    continue
  fi
  msg=`echo $line | awk '{split($0, a, ": "); print a[3]}'`
  case $type in
    "Latency")  latency_list=$latency_list`echo $msg | awk '{split($0, a, " "); print ", "a[1]", "a[4]}'`;;
    "Throughput") throughput=`echo $msg | awk '{print $1}'` ;;
    "Average Latency") avg_latency=`echo $msg | awk '{print $1}'` ;;
    "Config") config=`echo $msg | awk '{print $1}'` ;;
    *) echo "Unknown type" ;;
  esac
done < $log_file

out_line=$last_workload", "$throughput", "$avg_latency", "$config$latency_list
echo $out_line
