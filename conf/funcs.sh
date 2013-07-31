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

############### common functions ################


if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
  RMDIR_CMD="fs -rm -r"
  DUS_CMD="fs -du -s"
else
  RMDIR_CMD="fs -rmr"
  DUS_CMD="fs -dus"
fi

FORMATS="%-12s %-10s %-8s %-20s %-20s %-20s %-20s\n"
function timestamp(){
    sec=`date +%s`
    nanosec=`date +%N`
    tmp=`expr $sec \* 1000 `
    msec=`expr $nanosec / 1000000 `
    echo `expr $tmp + $msec`
}

function print_field_name() {
    printf "$FORMATS" Type Date Time Input_data_size "Duration(s)" "Throughput(bytes/s)" Throughput/node > $HIBENCH_REPORT
}

function gen_report() {
    local type=$1
    local start=$2
    local end=$3
    local size=$4
    which bc > /dev/null 2>&1
    if [ $? -eq 1 ]; then
        echo "\"bc\" utility missing. Please install it to generate proper report."
        return 1
    fi
    local duration=$(echo "scale=3;($end-$start)/1000"|bc)
    local tput=`echo "$size/$duration"|bc`

    if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then
      local nodes=`$MAPRED_EXECUTABLE job -list-active-trackers | wc -l`
    else
      local nodes=`$HADOOP_EXECUTABLE job -list-active-trackers | wc -l`
    fi

    local tput_node=`echo "$tput/$nodes"|bc`

    if [ ! -f $HIBENCH_REPORT ] ; then
        print_field_name
    fi

    printf "$FORMATS" $type $(date +%F) $(date +%T) $size $duration $tput $tput_node >> $HIBENCH_REPORT
}

function check_dir() {
    local dir=$1
    if [ -z "$dir" ];then
        echo "WARN: payload missing."
        return 1
    fi
    if [ ! -d "$dir" ];then
        echo "ERROR: directory $dir does not exist."
        exit 1
    fi
}

function dir_size() {
    for item in $($HADOOP_EXECUTABLE $DUS_CMD $1); do
        if [[ $item =~ ^[0-9]+$ ]]; then
            echo $item
        fi
    done
}


function check_compress() {
  if [ "x"$HADOOP_VERSION == "xhadoop2" ]; then

     if [ $COMPRESS -eq 1 ]; then
       COMPRESS_OPT="-D mapreduce.map.output.compress=true \
                     -D mapreduce.map.output.compress.codec=$COMPRESS_CODEC \
                     -D mapreduce.output.fileoutputformat.compress=true \
                     -D mapreduce.output.fileoutputformat.compress==$COMPRESS_CODEC \
                     -D mapreduce.output.fileoutputformat.compress.type=BLOCK "
     else
       COMPRESS_OPT="-D mapreduce.map.output.compress=false \
                     -D mapreduce.output.fileoutputformat.compress=false "
     fi

  else
    if [ $COMPRESS -eq 1 ]; then

      COMPRESS_OPT="-D mapred.output.compress=true \
                    -D mapred.output.compression.codec=$COMPRESS_CODEC \
                    -D mapred.output.compression.type=BLOCK "

    else
      COMPRESS_OPT="-D mapred.output.compress=false"
    fi
  fi
}
