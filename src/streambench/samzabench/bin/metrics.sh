#!/bin/sh

set -u
bin=`dirname "$0"`
bin=`cd "$bin";pwd`
DIR=`cd $bin/../; pwd`
. "${DIR}/conf/configure.sh"
echo "=========start samza metrics========="

if [ ! -d $DIR/target ]; then
  echo "Please run maven first."
  exit 1
fi

if [ ! -d $DIR/target/samza ]; then
  mkdir $DIR/target/samza
  tar zxf $DIR/target/*.tar.gz -C $DIR/target/samza
fi

CLASSPATH=$HADOOP_CONF_DIR
for file in $DIR/target/samza/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

JAVA_OPTS="-Dlog4j.configuration=file:$DIR/target/samza/lib/log4j.xml -Dsamza.log.dir=$DIR/target"
configFactory=org.apache.samza.config.factories.PropertiesConfigFactory
configFile=$DIR/conf/metrics.properties

echo $JAVA $JAVA_OPTS -cp $CLASSPATH org.apache.samza.job.JobRunner --config-factory=$configFactory --config-path=file://$configFile
exec $JAVA $JAVA_OPTS -cp $CLASSPATH org.apache.samza.job.JobRunner --config-factory=$configFactory --config-path=file://$configFile
