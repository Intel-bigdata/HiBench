#!/bin/sh

DIR=`dirname "$0"`

# Run the package configure
#. $DIR/../configure.sh

if [ $? != 0 ] ;then
  echo "Please make sure $DIR/../configure.sh executes correctly."
  exit 1;
fi

# Run the workload configure
. $DIR/../../bin/hibench-config.sh
. $DIR/../conf/configure.sh

# Run tpch benchmark
# hive all benchmark queries
export HADOOP_HOME=${HADOOP_EXECUTABLE%/bin*}
#for function in DM_I DM_C DM_CA DM_CC DM_W DM_P DM_WS DM_WP LF_WS LF_WR LF_I DF_I DF_WS
for function in LF_I
do
	$HIVE_HOME/bin/hive -f $DIR/hive/$function.hive 
done
unset HADOOP_HOME
exit
