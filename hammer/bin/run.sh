#!/bin/sh

DIR=$(cd `dirname "$0"`; pwd)

# global config
. $DIR/../../bin/hibench-config.sh

# local config
. $DIR/../conf/configure.sh

cd $DIR/..
HAMMER_HOME=$DIR/..
echo "HAMMER - START RUNNING" >> $HAMMER_HOME/hammer.report
START_TIME=`timestamp`

#
# Refresh data
#
# Reresh sales data
COUNT=1
export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
while (( COUNT <= REFRESHES ))
do
    # Reresh sales data
    REFRESH_SALES_START_TIME=`timestamp`
    
    echo "refreshing sales data ..................."
    for function in DM_I DM_C DM_CA DM_CC DM_W DM_P DM_WS DM_WP LF_WS LF_WR LF_I DF_I DF_WS
    do
        $HIVE_HOME/bin/hive -d SALES_DATA=$HAMMER_HDFS_BASE/etl-sales-db/DATA -f $DIR/hive/$function.hive 
    done
    REFRESH_SALES_END_TIME=`timestamp`
    echo -e  "REFRESH SALES DATA ROUND${COUNT}\t${REFRESH_SALES_START_TIME}\t${REFRESH_SALES_END_TIME}" >> $HAMMER_HOME/hammer.report
    # Refresh web log data
    REFRESH_LOGS_START_TIME=`timestamp`

    echo "refreshing web log data ..................."
    $HIVE_HOME/bin/hive -f $DIR/hive/DM_LOGS.hive
    REFRESH_LOGS_END_TIME=`timestamp`
    echo -e  "REFRESH LOGS ROUND${COUNT}\t${REFRESH_LOGS_START_TIME}\t${REFRESH_LOGS_END_TIME}" >> $HAMMER_HOME/hammer.report

    let COUNT++
done
unset HADOOP_HOME

# Generate preference
echo "generate preference ..................."
GENERATE_PREF_START_TIME=`timestamp`

date2stamp () {
    date --utc --date "$1" +%s
}

stamp2date (){
    date --utc --date "1970-01-01 $1 sec" "+%Y-%m-%d %T"
}

dateDiff (){
    case $1 in
        -s)   sec=1;      shift;;
        -m)   sec=60;     shift;;
        -h)   sec=3600;   shift;;
        -d)   sec=86400;  shift;;
        *)    sec=86400;;
    esac
    dte1=$(date2stamp $1)
    dte2=$(date2stamp $2)
    diffSec=$((dte2-dte1))
    if ((diffSec < 0)); then abs=-1; else abs=1; fi
    echo $((diffSec/sec*abs))
}

TRAIN_STARTDATE=$[$(dateDiff -d ${TRAINSTARTDATE} "1998-01-01") + 2450815]
TRAIN_ENDDATE=$[$(dateDiff -d ${TRAINENDDATE} "1998-01-01") + 2450815]

export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
$HIVE_HOME/bin/hive -f $DIR/hive/user_item_pref.hive -d TRAIN_STARTDATE=${TRAIN_STARTDATE} -d TRAIN_ENDDATE=${TRAIN_ENDDATE}
unset HADOOP_HOME
GENERATE_PREF_END_TIME=`timestamp`
echo -e  "GENERATE PREFERENCE\t${GENERATE_PREF_START_TIME}\t${GENERATE_PREF_END_TIME}" >> $HAMMER_HOME/hammer.report

# Generate recommendation
echo "generate recommendation..................."
GENERATE_RECOMM_START_TIME=`timestamp`

RECOMMENDATION_BASE=${HAMMER_HDFS_BASE}/recommendation
$HADOOP_EXECUTABLE fs -rmr $RECOMMENDATION_BASE
$HADOOP_EXECUTABLE jar $MAHOUT_HOME/core/target/mahout-core-0.7-job.jar \
    org.apache.mahout.cf.taste.hadoop.item.RecommenderJob \
    -D mapred.reduce.tasks=8 \
    --input "/user/hive/warehouse/etl_sales_db.db/user_item_pref" \
    --output "$RECOMMENDATION_BASE/output" \
    --tempDir "$RECOMMENDATION_BASE/tmp" \
    --numRecommendations $NUM_RECOMMENDATION \
    --similarityClassname SIMILARITY_COOCCURRENCE
GENERATE_RECOMM_END_TIME=`timestamp`
echo -e  "GENERATE RECOMMENDATION\t${GENERATE_RECOMM_START_TIME}\t${GENERATE_RECOMM_END_TIME}" >> $HAMMER_HOME/hammer.report

#
# Test
#
echo "test ..................."
TEST_START_TIME=`timestamp`
$HADOOP_EXECUTABLE fs -rmr $RECOMMENDATION_BASE/output/_SUCCESS
$HADOOP_EXECUTABLE fs -rmr $RECOMMENDATION_BASE/output/_logs

TEST_STARTDATE=$[$(dateDiff -d ${TESTSTARTDATE} "1998-01-01") + 2450815]
TEST_ENDDATE=$[$(dateDiff -d ${TESTENDDATE} "1998-01-01") + 2450815]
export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
$HIVE_HOME/bin/hive -d TEST_STARTDATE=${TEST_STARTDATE} -d TEST_ENDDATE=${TEST_ENDDATE} -d HDFS_PATH=$RECOMMENDATION_BASE/output -f $DIR/hive/test.hive
unset HADOOP_HOME
TEST_END_TIME=`timestamp`
echo -e  "TEST\t${TEST_START_TIME}\t${TEST_END_TIME}" >> $HAMMER_HOME/hammer.report

#
# Report
#
END_TIME=`timestamp`
gen_report "HAMMER" ${START_TIME} ${END_TIME} ${SCALE}
