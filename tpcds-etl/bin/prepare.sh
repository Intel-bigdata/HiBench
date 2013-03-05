#!/bin/bash

bin=`dirname "$0"`
bin=`cd "#bin"; pwd`

echo "========== preparing tpcds data =========="
echo "start configure `date`"
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"
DBGEN_DIR="${DIR}/dbgen"
echo "start create input `date`"
# prepare mapreduce input
if [ -d "${DBGEN_DIR}/Input" ]
then
  rm -f ${DBGEN_DIR}/Input/*
else
  mkdir ${DBGEN_DIR}/Input
fi

for child in $(seq ${PARALLEL});do
  echo "${child}" > ${DBGEN_DIR}/Input/${child}.txt
done

echo "start mkdir `date`"
${HADOOP_EXECUTABLE} fs -rmr ${DBGEN_BASE_HDFS}
${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_BASE_HDFS}
${HADOOP_EXECUTABLE} fs -moveFromLocal ${DBGEN_DIR}/Input ${DBGEN_BASE_HDFS}
${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_DATA}

TMP_DIR_STR=""
for tnp in $TABLES_NO_PARALLEL ; do
  ${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_DATA}/${tnp} &
done

for tp in $TABLES_PARALLEL ; do
  ${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_DATA}/${tp} &
done

for rt in s_customer s_customer_address s_inventory s_item s_promotion delete s_web_order s_web_order_lineitem s_web_page s_web_returns s_web_site s_call_center s_warehouse s_zip_to_gmt
do
  ${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_DATA}/${rt} &
done
wait

echo "start mapreduce create data file and upload to hdfs `date`"
# generate data
cd ${DBGEN_DIR}
OPTION="-input ${DBGEN_INPUT} \
-output ${DBGEN_OUTPUT} \
-mapper ./run.sh \
-file ${DBGEN_HOME}/tools/dsdgen -file run.sh -file ${DBGEN_HOME}/tools/tpcds.idx -file ${DIR}/conf/configure.sh \
-jobconf mapred.reduce.tasks=0 \
-jobconf mapred.job.name=prepare_data_tpcds_etl \
-jobconf mapred.task.timeout=${TIMEOUT}"

# add to hibench-config.sh
if [ -z "$STREAMING" ]; then
    export STREAMING=`dirname ${HADOOP_EXAMPLES_JAR}`/contrib/streaming/hadoop-streaming-*.jar
fi

START_TIME=`timestamp`
echo "start generating data"
${HADOOP_EXECUTABLE} jar ${STREAMING} ${OPTION}

echo "start loading to hive `date`"
# Create tables
sed "s#TPCDS_DATA_DIR#${DBGEN_DATA}#g" $DIR/bin/hive/tpcds.template > $DIR/bin/hive/tpcds.hive
cd ${DIR}/bin
$HIVE_HOME/bin/hive -f $DIR/bin/hive/tpcds.hive
echo $HIVE_HOME/bin/hive
echo "start loading refresh data `date`"
# Create refresh tables
sed "s#TPCDS_DATA_DIR#${DBGEN_DATA}#g" $DIR/bin/hive/tpcds_source.template > $DIR/bin/hive/tpcds_source.hive
pwd
$HIVE_HOME/bin/hive -f $DIR/bin/hive/tpcds_source.hive

echo "all finish `date`"
END_TIME=`timestamp`

echo "all finished"
