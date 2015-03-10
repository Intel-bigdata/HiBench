#bin directory of spark installation (will use spark-submit)
SPARK_BIN_DIR=

###############

#Cluster config
# Spark master location
sparkMaster=

# zookeeper host of kafka cluster
zkHost=

###############

#Parallel config
# number of nodes that will receive kafka input
receiverNodes=

###############
#Benchmark args
#Note to ensure benchName to be consistent with datagen type. Numeric data for statistics and text data for others
# please uncomment one benchName to run the benchmark
#benchName="micro/identity"
#benchName="micro/sample"
#benchName="micro/sketch"
#benchName="micro/grep"
#benchName="micro/wordcount"
#benchName="micro/distinctcount"
#benchName="micro/statistics"

#common args
# the topic that spark will receive input data
topicName=
# Spark stream batch interval
batchInterval=  #In seconds
# consumer group of the spark consumer for kafka
consumerGroup=
# expected number of records to be processed
recordCount=

#sketch/distinctcount/statistics arg
# the field index of the record that will be extracted
fieldIndex=1

#sketch/wordcount/distinctcount/statistics arg
# the seperator between fields of a single record
separator=\\s+

#sample arg
# probability that a record will be taken as a sample
prob=0.1

#grep arg
# the substring that will be checked to see if contained in a record
pattern=the

#common arg
# indicate RDD storage level. 
# 1 for memory only 1 copy. Others for default mem_disk_ser 2 copies 
copies=2

# indicate whether to test the write ahead log new feature
# set true to test WAL feature
testWAL=false

# if testWAL is true, this path to store stream context in hdfs shall be specified. If false, it can be empty
checkpointPath=

#common arg
# indicate whether in debug mode for correctness verfication
debug=false

