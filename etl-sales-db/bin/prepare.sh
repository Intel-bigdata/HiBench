#!/bin/bash

bin=`dirname "$0"`
bin=`cd "#bin"; pwd`

echo "========== preparing tpcds data =========="

# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"
DBGEN_LOCAL_DIR="${DIR}/dbgen"
DBGEN_HDFS_BASE=${ETLRECOMM_HDFS_BASE}/etl-sales-db
DBGEN_HDFS_INPUT=${DBGEN_HDFS_BASE}/Input
DBGEN_HDFS_OUTPUT=${DBGEN_HDFS_BASE}/Output
DBGEN_HDFS_DATA=${DBGEN_HDFS_BASE}/DATA

# prepare mapreduce input
if [ -d "${DBGEN_LOCAL_DIR}/Input" ]
then
  rm -f ${DBGEN_LOCAL_DIR}/Input/*
else
  mkdir ${DBGEN_LOCAL_DIR}/Input
fi

for CHILD in $(seq ${PARALLEL});do
  echo "${CHILD}" > ${DBGEN_LOCAL_DIR}/Input/${CHILD}.txt
done

echo $DBGEN_HDFS_DATA

${HADOOP_EXECUTABLE} fs -rmr ${DBGEN_HDFS_BASE}
${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_HDFS_DATA}
${HADOOP_EXECUTABLE} fs -moveFromLocal ${DBGEN_LOCAL_DIR}/Input ${DBGEN_HDFS_INPUT}

for TNP in dbgen_version date_dim time_dim call_center income_band household_demographics item warehouse promotion reason ship_mode store web_site web_page 
do
  ${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_HDFS_DATA}/${TNP} &
done

for TP in customer customer_address customer_demographics inventory web_sales web_returns
do
  ${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_HDFS_DATA}/${TP} &
done

for RT in s_customer s_customer_address s_inventory s_item s_promotion delete s_web_order s_web_order_lineitem s_web_page s_web_returns s_web_site s_call_center s_warehouse s_zip_to_gmt
do
  ${HADOOP_EXECUTABLE} fs -Ddfs.umask=0000 -mkdir ${DBGEN_HDFS_DATA}/${RT} &
done
wait

# generate data
cd ${DBGEN_LOCAL_DIR}
OPTION="-input ${DBGEN_HDFS_INPUT} \
-output ${DBGEN_HDFS_OUTPUT} \
-mapper ./run.sh \
-file ${DBGEN_HOME}/tools/dsdgen -file run.sh -file ${DBGEN_HOME}/tools/tpcds.idx -file ${DIR}/conf/configure.sh \
-jobconf mapred.reduce.tasks=0 \
-jobconf mapred.job.name=prepare_etl_sales_db \
-jobconf mapred.task.timeout=0"

# export streaming
if [ -z "$STREAMING" ]; then
    export STREAMING=`dirname ${HADOOP_EXAMPLES_JAR}`/contrib/streaming/hadoop-streaming-*.jar
fi

START_TIME=`timestamp`

# echo "start generating data"
${HADOOP_EXECUTABLE} jar ${STREAMING} ${OPTION}

# Create tables
sed "s#ETL_SALES_DB_DIR#${DBGEN_HDFS_DATA}#g" $DIR/bin/hive/create_base.template > $DIR/bin/hive/create_base.hive
cd ${DIR}/bin
$HIVE_HOME/bin/hive -f $DIR/bin/hive/create_base.hive
echo "start loading refresh data `date`"

# Create refresh tables
sed "s#ETL_SALES_DB_DIR#${DBGEN_HDFS_DATA}#g" $DIR/bin/hive/create_refresh.template > $DIR/bin/hive/create_refresh.hive
$HIVE_HOME/bin/hive -f $DIR/bin/hive/create_refresh.hive

END_TIME=`timestamp`
