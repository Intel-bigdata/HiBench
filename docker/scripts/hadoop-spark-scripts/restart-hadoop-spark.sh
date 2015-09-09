# restart ssh service
service ssh restart

# stop process
${HADOOP_HOME}/sbin/stop-dfs.sh
${HADOOP_HOME}/sbin/stop-yarn.sh
${SPARK_HOME}/sbin/stop-all.sh

# clear data directories
mkdir -p /usr/local/hdfs/namenode/
mkdir -p /usr/local/hdfs/datanode/
rm -fr /usr/local/hdfs/namenode/*
rm -fr /usr/local/hdfs/datanode/*

# remove related logs
rm -fr ${HADOOP_HOME}/logs/*

# hdfs format
${HADOOP_HOME}/bin/hdfs namenode -format

# restart hdfs
${HADOOP_HOME}/sbin/start-dfs.sh

# restart yarn
${HADOOP_HOME}/sbin/start-yarn.sh

# restart spark
${SPARK_HOME}/sbin/start-all.sh

# build HiBench
echo "Start building HiBench ..."
/root/HiBench-${HIBENCH_VERSION}-branch/bin/build-all.sh

echo "#======================================================================#"
echo "       Now you can run all workload by:                                 "
echo "                                                                        "
echo "         /root/HiBench-${HIBENCH_VERSION}-branch/bin/run-all.sh         "
echo "#======================================================================#"
