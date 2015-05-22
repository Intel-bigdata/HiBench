###############

#Benchmark args
# Note to ensure benchName to be consistent with datagen type. Numeric data for statistics and text data for others
# please  uncomment one benchName to run the benchmark
#benchName=micro-identity
#benchName=micro-sample
#benchName=micro-sketch
#benchName=micro-grep
benchName=micro-wordcount
#benchName=micro-distinctcount
#benchName=micro-statisticssep

#Yarn args
HADOOP_YARN_HOME=/home/shilei/works-2.0/sync/hadoop/hadoop-2.3.0-cdh5.1.3
HADOOP_CONF_DIR=$HADOOP_YARN_HOME/etc/hadoop


