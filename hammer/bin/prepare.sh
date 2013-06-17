#!/bin/bash

# use HiBench root as working directory 
# use absolute path in all places

HAMMER_HOME=$(cd $(dirname "$0")/..;pwd)
echo "HAMMER - START PREPARING" >> $HAMMER_HOME/hammer.report

# HiBench configuration
. "$HAMMER_HOME/../bin/hibench-config.sh"

# local configuration
. "$HAMMER_HOME/conf/configure.sh"

# check for existence of hadoop streaming
export STREAMING=

if [ -n "$HADOOP_HOME" ]; then
    # for hadoop 1.0.x
    if [ -z "$STREAMING" ] && [ -e "$HADOOP_HOME/contrib/streaming/hadoop-streaming-*.jar" ]; then
      export STREAMING=$HADOOP_HOME/contrib/streaming/hadoop-streaming-*.jar
    fi
    # for hadoop 2.0.x
    if [ -z "$STREAMING" ] && [ -e "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar" ]; then
      export STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
    fi
    # for other hadoop version
    if [ -z "$STREAMING" ]; then
      export STREAMING=`find $HADOOP_HOME -name hadoop-stream*.jar -type f`
    fi
else
    # for hadoop 1.0.x
    if [ -z "$STREAMING" ] && [ -e `dirname ${HADOOP_EXAMPLES_JAR}`/contrib/streaming/hadoop-streaming-*.jar ]; then
      export STREAMING=`dirname ${HADOOP_EXAMPLES_JAR}`/contrib/streaming/hadoop-streaming-*.jar
    fi
    # for hadoop 2.0.x
    if [ -z "$STREAMING" ] && [ -e `dirname ${HADOOP_EXAMPLES_JAR}`/../tools/lib/hadoop-streaming-*.jar ]; then
      export STREAMING=`dirname ${HADOOP_EXAMPLES_JAR}`/../tools/lib/hadoop-streaming-*.jar
    fi
fi

if [ -z "$STREAMING" ]; then
    echo 'Hammer can not find hadoop-streaming jar file, please set STREAMING path.'
    exit
fi

# check for existence of dsgen tool
if [ -z $DSGEN_HOME ] ; then 
    echo "Please set DSGEN_HOME in conf/configure.sh."
    exit
elif [ ! -d $DSGEN_HOME ] ; then
    echo "$DSGEN_HOME doesn't exist or is not a directory."
    exit
fi
    
cd $HAMMER_HOME

#####################
# generate sales data
#####################
GENERATE_SALES_START_TIME=`timestamp`

DBGEN_LOCAL_DIR=${HAMMER_HOME}/dbgen
DBGEN_HDFS_BASE=${HAMMER_HDFS_BASE}/etl-sales-db
DBGEN_HDFS_INPUT=${DBGEN_HDFS_BASE}/Input
DBGEN_HDFS_OUTPUT=${DBGEN_HDFS_BASE}/Output
DBGEN_HDFS_DATA=${DBGEN_HDFS_BASE}/DATA

# prepare mapreduce input
if [ -d "${DBGEN_LOCAL_DIR}/Input" ]
then
  rm -f ${DBGEN_LOCAL_DIR}/Input/*
else
  mkdir -p ${DBGEN_LOCAL_DIR}/Input
fi

for CHILD in $(seq ${PARALLEL});do
  echo "${CHILD}" > ${DBGEN_LOCAL_DIR}/Input/${CHILD}.txt
done

echo $DBGEN_HDFS_DATA

# clean up previous data if available
if ${HADOOP_EXECUTABLE} fs -test -e ${DBGEN_HDFS_BASE} ; then
    ${HADOOP_EXECUTABLE} fs -rmr ${DBGEN_HDFS_BASE}
fi

${HADOOP_EXECUTABLE} fs -Dfs.permissions.umask-mode=000 -mkdir ${DBGEN_HDFS_DATA}
${HADOOP_EXECUTABLE} fs -moveFromLocal ${DBGEN_LOCAL_DIR}/Input ${DBGEN_HDFS_INPUT}

for TNP in dbgen_version date_dim time_dim call_center income_band household_demographics item warehouse promotion reason ship_mode store web_site web_page 
do
  ${HADOOP_EXECUTABLE} fs -Dfs.permissions.umask-mode=000 -mkdir ${DBGEN_HDFS_DATA}/${TNP} &
done

for TP in customer customer_address customer_demographics inventory web_sales web_returns
do
  ${HADOOP_EXECUTABLE} fs -Dfs.permissions.umask-mode=000 -mkdir ${DBGEN_HDFS_DATA}/${TP} &
done

for RT in s_customer s_customer_address s_inventory s_item s_promotion delete s_web_order s_web_order_lineitem s_web_page s_web_returns s_web_site s_call_center s_warehouse s_zip_to_gmt
do
  ${HADOOP_EXECUTABLE} fs -Dfs.permissions.umask-mode=000 -mkdir ${DBGEN_HDFS_DATA}/${RT} &
done
wait

# generate raw sales data
OPTION="-input ${DBGEN_HDFS_INPUT} \
-output ${DBGEN_HDFS_OUTPUT} \
-mapper $HAMMER_HOME/bin/dbgen.sh \
-file ${DSGEN_HOME}/tools/dsdgen -file $HAMMER_HOME/bin/dbgen.sh -file ${DSGEN_HOME}/tools/tpcds.idx -file ${HAMMER_HOME}/conf/configure.sh \
-jobconf mapred.reduce.tasks=0 \
-jobconf mapred.job.name=prepare_etl_sales_db \
-jobconf mapred.task.timeout=0"

${HADOOP_EXECUTABLE} jar ${STREAMING} ${OPTION}

# import sales data to hive tables
$HIVE_HOME/bin/hive -d ETL_SALES_DB_DIR=${DBGEN_HDFS_DATA} -f $HAMMER_HOME/bin/hive/create_base.hive

# import sales refresh data to hive tables
$HIVE_HOME/bin/hive -d ETL_SALES_DB_DIR=${DBGEN_HDFS_DATA} -f $HAMMER_HOME/bin/hive/create_refresh.hive

GENERATE_SALES_END_TIME=`timestamp`
echo -e "GENERATE SALES DATA\t${GENERATE_SALES_START_TIME}\t${GENERATE_SALES_END_TIME}" >> $HAMMER_HOME/hammer.report

#######################
# generate web log data
#######################
GENERATE_LOGS_START_TIME=`timestamp`

# create web log generator input
WEBLOG_OUTPUT_HDFS=${HAMMER_HDFS_BASE}/weblog/Output
if ${HADOOP_EXECUTABLE} fs -test -e ${WEBLOG_OUTPUT_HDFS} ; then
    ${HADOOP_EXECUTABLE} fs -rmr ${WEBLOG_OUTPUT_HDFS}
fi
export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
$HIVE_HOME/bin/hive -f $HAMMER_HOME/bin/hive/create_base_logs_raw.hive -d LOG_HOME=${WEBLOG_OUTPUT_HDFS}
unset HADOOP_HOME

# generate raw web log data
${HADOOP_EXECUTABLE} fs -rmr ${WEBLOG_OUTPUT_HDFS}/web_logs
${HADOOP_EXECUTABLE} fs -mkdir ${WEBLOG_OUTPUT_HDFS}/cookies
${HADOOP_EXECUTABLE} fs -mkdir ${WEBLOG_OUTPUT_HDFS}/ip
${HADOOP_EXECUTABLE} fs -mkdir ${WEBLOG_OUTPUT_HDFS}/useragents
CUSTOMERS_MAX=`${HIVE_HOME}/bin/hive -e 'select max(c_customer_sk) from etl_sales_db.customer'`
ITEMS_MAX=`${HIVE_HOME}/bin/hive -e 'select max(i_item_sk) from etl_sales_db.item'`
${HADOOP_EXECUTABLE} jar ${HAMMER_HOME}/lib/Hammer-java.jar com.intel.hammer.log.generator.hadoop.LogGenJob ${WEBLOG_OUTPUT_HDFS}/web_logs_input ${WEBLOG_OUTPUT_HDFS}/web_logs ${PARTNUM} $CUSTOMERS_MAX $ITEMS_MAX
${HADOOP_EXECUTABLE} fs -mv ${WEBLOG_OUTPUT_HDFS}/web_logs/cookies-* ${WEBLOG_OUTPUT_HDFS}/cookies/
${HADOOP_EXECUTABLE} fs -mv ${WEBLOG_OUTPUT_HDFS}/web_logs/useragents-* ${WEBLOG_OUTPUT_HDFS}/useragents/
${HADOOP_EXECUTABLE} fs -mv ${WEBLOG_OUTPUT_HDFS}/web_logs/ip-* ${WEBLOG_OUTPUT_HDFS}/ip/

# import web log data to hive tables
export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
$HIVE_HOME/bin/hive -f $HAMMER_HOME/bin/hive/create_base_logs_tables.hive -d LOG_HOME=${WEBLOG_OUTPUT_HDFS}
unset HADOOP_HOME

# create web_logs refresh tables
REFRESH_SOURCE_HDFS=${HAMMER_HDFS_BASE}/weblog/Refresh
if ${HADOOP_EXECUTABLE} fs -test -e ${REFRESH_SOURCE_HDFS} ; then
    ${HADOOP_EXECUTABLE} fs -rmr ${REFRESH_SOURCE_HDFS}
fi
export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
$HIVE_HOME/bin/hive -f $HAMMER_HOME/bin/hive/create_refresh_logs_raw.hive -d REFRESH_HOME=${REFRESH_SOURCE_HDFS}
unset HADOOP_HOME

# generate raw web log refersh data
${HADOOP_EXECUTABLE} fs -rmr ${REFRESH_SOURCE_HDFS}/s_web_logs
${HADOOP_EXECUTABLE} fs -mkdir ${REFRESH_SOURCE_HDFS}/s_cookies
${HADOOP_EXECUTABLE} fs -mkdir ${REFRESH_SOURCE_HDFS}/s_ip
${HADOOP_EXECUTABLE} fs -mkdir ${REFRESH_SOURCE_HDFS}/s_useragents
CUSTOMERS_MAX=`${HIVE_HOME}/bin/hive -e 'select max(c_customer_sk) from etl_sales_db.customer'`
ITEMS_MAX=`${HIVE_HOME}/bin/hive -e 'select max(i_item_sk) from etl_sales_db.item'`
${HADOOP_EXECUTABLE} jar ${HAMMER_HOME}/lib/Hammer-java.jar com.intel.hammer.log.generator.hadoop.LogGenJob ${REFRESH_SOURCE_HDFS}/s_web_logs_input ${REFRESH_SOURCE_HDFS}/s_web_logs ${PARTNUM} $CUSTOMERS_MAX $ITEMS_MAX
${HADOOP_EXECUTABLE} fs -mv ${REFRESH_SOURCE_HDFS}/s_web_logs/cookies-* ${REFRESH_SOURCE_HDFS}/s_cookies/
${HADOOP_EXECUTABLE} fs -mv ${REFRESH_SOURCE_HDFS}/s_web_logs/useragents-* ${REFRESH_SOURCE_HDFS}/s_useragents/
${HADOOP_EXECUTABLE} fs -mv ${REFRESH_SOURCE_HDFS}/s_web_logs/ip-* ${REFRESH_SOURCE_HDFS}/s_ip/

GENERATE_LOGS_END_TIME=`timestamp`
echo -e "GENERATE LOGS\t${GENERATE_LOGS_START_TIME}\t${GENERATE_LOGS_END_TIME}" >> $HAMMER_HOME/hammer.report

