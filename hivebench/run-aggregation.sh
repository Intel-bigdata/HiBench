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

#!/bin/bash

echo "========== running hive-aggregate bench =========="
# configure
DIR=`dirname "$0"`
source ${DIR}/../funcs.sh
configure ${DIR}

# path check
rm -rf metastore_db
rm -rf TempStatsStore
$HADOOP_HOME/bin/hadoop dfs -rmr /user/hive/warehouse/uservisits_aggre/*
$HADOOP_HOME/bin/hadoop dfs -rmr /tmp/*

# pre-running
echo "USE DEFAULT;" > $DIR/hive-benchmark/uservisits_aggre.hive
echo "set mapred.map.tasks=$NUM_MAPS;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "set mapred.reduce.tasks=$NUM_REDS;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "set hive.stats.autogather=false;" >> $DIR/hive-benchmark/uservisits_aggre.hive

if [ $COMPRESS -eq 1 ]; then
    echo "set mapred.output.compress=true;" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set hive.exec.compress.output=true;" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapred.output.compression.type=BLOCK;" >> $DIR/hive-benchmark/uservisits_aggre.hive
    echo "set mapred.output.compression.codec=$COMPRESS_CODEC;" >> $DIR/hive-benchmark/uservisits_aggre.hive
fi

echo "DROP TABLE uservisits;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "DROP TABLE uservisits_aggre;" >> $DIR/hive-benchmark/uservisits_aggre.hive
echo "CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';">> $DIR/hive-benchmark/uservisits_aggre.hive
cat $DIR/hive-benchmark/uservisits_aggre.template >> $DIR/hive-benchmark/uservisits_aggre.hive

SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS/uservisits | awk '{ print $2 }'`
START_TIME=`timestamp`

# run bench
$HIVE_HOME/bin/hive -f $DIR/hive-benchmark/uservisits_aggre.hive

# post-running
END_TIME=`timestamp`
gen_report "HIVEAGGR" ${START_TIME} ${END_TIME} ${SIZE} >> ${HIBENCH_REPORT}

$HADOOP_HOME/bin/hadoop fs -rmr $OUTPUT_HDFS/hive-aggre
$HADOOP_HOME/bin/hadoop fs -cp /user/hive/warehouse/uservisits_aggre $OUTPUT_HDFS/hive-aggre
