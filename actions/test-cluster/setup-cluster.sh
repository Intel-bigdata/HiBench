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
cp ./hadoop-env.sh ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
cp ./log4j.properties ~/opt/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION/conf
cp ./spark-defaults.conf ~/opt/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION/conf
if [ $HADOOP_VERSION = "3.2.1" ]
then
    cp ./hadoop-layout.sh ~/opt/hadoop-${HADOOP_VERSION}/libexec
    cp ./mapred-site_3.2.1.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/mapred-site.xml
    cp ./yarn-site_3.2.1.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/yarn-site.xml
else
    cp ./mapred-site.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
    cp ./yarn-site.xml ~/opt/hadoop-$HADOOP_VERSION/etc/hadoop/
fi

echo $HOST_IP > $HADOOP_HOME/etc/hadoop/slaves
echo $HOST_IP > $SPARK_HOME/conf/slaves

# create directories
mkdir -p /tmp/run/hdfs/namenode
mkdir -p /tmp/run/hdfs/datanode

# hdfs format
$HADOOP_HOME/bin/hdfs namenode -format

#wget -P $HADOOP_HOME/share/hadoop/yarn/lib/ https://repo1.maven.org/maven2/javax/activation/activation/1.1.1/activation-1.1.1.jar

# start hdfs and yarn
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh


jps
free -g
df -h
yarn application -list
ls -ls $HADOOP_HOME/logs/
cat $HADOOP_HOME/logs/hadoop-*-resourcemanager-*.log
cat $HADOOP_HOME/logs/hadoop-*-nodemanager-*.log

sleep 10
$HADOOP_HOME/bin/hadoop fs -ls /
$HADOOP_HOME/bin/yarn node -list 2
