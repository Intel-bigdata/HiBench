#!/bin/sh

set -u
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
DIR=`cd $bin/../; pwd`
SRC_DIR="$DIR/../../../src/streambench/samza"
. "${SRC_DIR}/conf/configure.sh"
echo "=========start samza metrics========="

if [ ! -d $SRC_DIR/target ]; then
  echo "Please run maven first."
  exit 1
fi

if [ ! -d $SRC_DIR/target/samza ]; then
  mkdir $SRC_DIR/target/samza
  tar zxf $SRC_DIR/target/*.tar.gz -C $SRC_DIR/target/samza
fi

CLASSPATH=$HADOOP_CONF_DIR
for file in $SRC_DIR/target/samza/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

JAVA_OPTS="-Dlog4j.configuration=file:$SRC_DIR/target/samza/lib/log4j.xml -Dsamza.log.dir=$SRC_DIR/target"
configFactory=org.apache.samza.config.factories.PropertiesConfigFactory
configFile=$SRC_DIR/conf/metrics.properties

echo $JAVA $JAVA_OPTS -cp $CLASSPATH org.apache.samza.job.JobRunner --config-factory=$configFactory --config-path=file://$configFile
exec $JAVA $JAVA_OPTS -cp $CLASSPATH org.apache.samza.job.JobRunner --config-factory=$configFactory --config-path=file://$configFile
