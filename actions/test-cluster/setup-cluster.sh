#!/usr/bin/env bash


# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo JAVA_HOME is $JAVA_HOME

# Setup password-less & python3
$GITHUB_WORKSPACE/actions/test-cluster/config-ssh.sh

# setup envs
source $SCRIPT_DIR/load-spark-envs.sh

# download spark & hadoop bins
[ -d ~/opt ] || mkdir ~/opt
cd ~/opt
[ -f spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION.tgz ] || wget --no-verbose https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION.tgz
[ -d spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION ] || tar -xzf spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION.tgz
[ -f hadoop-$HADOOP_VERSION.tar.gz ] || wget --no-verbose https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
[ -d hadoop-$HADOOP_VERSION ] || tar -xzf hadoop-$HADOOP_VERSION.tar.gz

cd $SCRIPT_DIR

HOST_IP=$(hostname -f)

sed -i "s/localhost/$HOST_IP/g" core-site.xml
sed -i "s/localhost/$HOST_IP/g" yarn-site.xml

cp ./core-site.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
cp ./hdfs-site.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
cp ./yarn-site.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
cp ./hadoop-env.sh ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
cp ./log4j.properties ~/opt/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION/conf
cp ./spark-defaults.conf ~/opt/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION/conf

echo $HOST_IP > $HADOOP_HOME/etc/hadoop/slaves
echo $HOST_IP > $SPARK_HOME/conf/slaves

ls -l $SPARK_HOME/conf

# create directories
mkdir -p /tmp/run/hdfs/namenode
mkdir -p /tmp/run/hdfs/datanode

# hdfs format
$HADOOP_HOME/bin/hdfs namenode -format

# start hdfs and yarn
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

hadoop fs -ls /
yarn node -list