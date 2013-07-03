#!/bin/bash 
bin=`dirname "$0"`
currpath=`cd "$bin"; pwd`

# configure
. configure.sh

HAMMER_HDFS_BASE=/HiBench/hammer
DBGEN_HDFS_BASE=${HAMMER_HDFS_BASE}/etl-sales-db
DBGEN_HDFS_DATA=${DBGEN_HDFS_BASE}/DATA

if [ -n $HADOOP_HOME ]; then
    HADOOP_EXECUTABLE=$HADOOP_HOME/bin/hadoop
else
    HADOOP_EXECUTABLE=`which hadoop`
fi

# begin generating
while read CHILD
do 
  mkdir -p data/child${CHILD}
	
  # create data file for parallel tables
  for TP in customer customer_address customer_demographics inventory web_sales 
  do
    echo table: $TP, child: $CHILD
    ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -table ${TP} -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
    if [ "${TP}" != "web_sales" ]
    then
      $HADOOP_EXECUTABLE fs -moveFromLocal data/child${CHILD}/${TP}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${TP}/${TP}_${CHILD}_${PARALLEL}.dat &
    else
      gzip data/child${CHILD}/${TP}_${CHILD}_${PARALLEL}.dat
      $HADOOP_EXECUTABLE fs -moveFromLocal data/child${CHILD}/${TP}_${CHILD}_${PARALLEL}.dat.gz ${DBGEN_HDFS_DATA}/${TP}/${TP}_${CHILD}_${PARALLEL}.dat.gz &
    fi
  done
	
  # create data file for parallel refresh data
    echo update tables, child: $CHILD
  ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -update 1 -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
  for RT in s_zip_to_gmt s_inventory s_web_order s_web_order_lineitem s_web_returns s_customer s_customer_address s_item s_promotion s_web_page s_web_site s_call_center s_warehouse
  do
    $HADOOP_EXECUTABLE fs -moveFromLocal data/child${CHILD}/${RT}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${RT}/${RT}_${CHILD}_${PARALLEL}.dat &
  done
	
  # create data file for no_parallel tables and refresh data
  if [ ${CHILD} -eq 1 ]
  then
    for RT in delete 
    do
      $HADOOP_EXECUTABLE fs -moveFromLocal data/child${CHILD}/${RT}_1.dat ${DBGEN_HDFS_DATA}/${RT}/${RT}_1.dat &
    done

    for TNP in dbgen_version date_dim time_dim call_center income_band household_demographics item warehouse promotion reason ship_mode store web_site web_page
    do
      echo table: $TP
      ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -table ${TNP} -terminate N 2>&1
      $HADOOP_EXECUTABLE fs -moveFromLocal data/child${CHILD}/${TNP}.dat ${DBGEN_HDFS_DATA}/${TNP}/${TNP}.dat &
    done
  fi
  wait
done 
