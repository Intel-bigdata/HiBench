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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== running nutchindex data =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

OUTPUT_HDFS=$INPUT_HDFS/indexes
SUBDIR=$1
if [ -n "$SUBDIR" ]; then
  OUTPUT_HDFS=$OUTPUT_HDFS"/"$SUBDIR
fi

NUTCH_DEPENDENCY_DIR=$DEPENDENCY_DIR"/nutchindexing/target/dependency"

if [ ! -e $DEPENDENCY_DIR"/nutchindexing/target/apache-nutch-1.2-bin.tar.gz" ]; then
  echo "Error: The nutch bin file hasn't be downloaded by maven, please check!"
  exit
fi

if [ $HADOOP_VERSION == "hadoop1" ]; then
  cp $DIR/nutch/conf/nutch-site-mr1.xml $DIR/nutch/conf/nutch-site.xml
elif [ $HADOOP_VERSION == "hadoop2" ]; then
  cp $DIR/nutch/conf/nutch-site-mr2.xml $DIR/nutch/conf/nutch-site.xml
fi

cd $DEPENDENCY_DIR"/nutchindexing/target"
if [ ! -d $NUTCH_HOME ]; then
  tar zxf apache-nutch-1.2-bin.tar.gz
fi

find $NUTCH_HOME/lib ! -name "lucene-*" -type f -exec rm -rf {} \;
cp $DIR/nutch/conf/nutch-site.xml $NUTCH_HOME/conf
cp $DIR/nutch/bin/nutch $NUTCH_HOME/bin


if [ $HADOOP_VERSION == "hadoop2" ]; then
  mkdir $NUTCH_HOME/temp
  unzip -q $NUTCH_HOME/nutch-1.2.job -d $NUTCH_HOME/temp
  rm $NUTCH_HOME/temp/lib/jcl-over-slf4j-*.jar
  cp $NUTCH_DEPENDENCY_DIR/jcl-over-slf4j-*.jar $NUTCH_HOME/temp/lib
  rm $NUTCH_HOME/nutch-1.2.job
  cd $NUTCH_HOME/temp
  zip -qr $NUTCH_HOME/nutch-1.2.job *
  rm -rf $NUTCH_HOME/temp
fi
cd $NUTCH_HOME

check_compress

# path check
$HADOOP_EXECUTABLE $RMDIR_CMD $OUTPUT_HDFS

# pre-running
SIZE=`dir_size $INPUT_HDFS`
#SIZE=`$HADOOP_EXECUTABLE fs -dus $INPUT_HDFS |  grep -o [0-9]* `
export NUTCH_CONF_DIR=$HADOOP_CONF_DIR:$NUTCH_HOME/conf
START_TIME=`timestamp`

# run bench
$NUTCH_HOME/bin/nutch index $COMPRESS_OPTS $OUTPUT_HDFS $INPUT_HDFS/crawldb $INPUT_HDFS/linkdb $INPUT_HDFS/segments/*

# post-running
END_TIME=`timestamp`
gen_report "NUTCHINDEX" ${START_TIME} ${END_TIME} ${SIZE}
