#!/bin/bash 
bin=`dirname "$0"`
currpath=`cd "$bin"; pwd`

# configure
. configure.sh

ETLRECOMM_HDFS_BASE=/HiBench/etl-recomm
DBGEN_HDFS_BASE=${ETLRECOMM_HDFS_BASE}/etl-sales-db
DBGEN_HDFS_DATA=${DBGEN_HDFS_BASE}/DATA

# begin generating
while read CHILD
do 
  mkdir -p data/child${CHILD}
	
  # create data file for parallel tables
  for TP in customer customer_address customer_demographics inventory web_sales web_returns
  do
    ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -table ${TP} -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
    if [ "${TP}" != "web_sales" ]
    then
      hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${TP}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${TP}/${TP}_${CHILD}_${PARALLEL}.dat &
    else
      gzip data/child${CHILD}/${TP}_${CHILD}_${PARALLEL}.dat
      hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${TP}_${CHILD}_${PARALLEL}.dat.gz ${DBGEN_HDFS_DATA}/${TP}/${TP}_${CHILD}_${PARALLEL}.dat.gz &
    fi
  done
	
  # create data file for parallel refresh data
  ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -update 1 -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
  for RT in s_zip_to_gmt s_inventory s_web_order s_web_order_lineitem s_web_returns s_customer s_customer_address s_item s_promotion s_web_page s_web_site s_call_center s_warehouse
  do
    hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${RT}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${RT}/${RT}_${CHILD}_${PARALLEL}.dat &
  done
	
  # create data file for no_parallel tables and refresh data
  if [ ${CHILD} -eq 1 ]
  then
    for RT in delete 
    do
      hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${RT}_1.dat ${DBGEN_HDFS_DATA}/${RT}/${RT}_1.dat &
    done

    for TNP in dbgen_version date_dim time_dim call_center income_band household_demographics item warehouse promotion reason ship_mode store web_site web_page
    do
      ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -table ${TNP} -terminate N 2>&1
      hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${TNP}.dat ${DBGEN_HDFS_DATA}/${TNP}/${TNP}.dat &
    done
  fi
  wait
done 
