#Please ensure the topic is created in kafka
# the kafka topic under which new data are generated
topic=

# Two data sets(text and numeric) are available, app argument indicates to use which
app=micro/sketch      #use text dataset, avg record size: 60 bytes
#app=micro/statistics #use numeric dataset, avg record size: 200 bytes

# Kafka brokers that data will be generated to
kafkabrokers=

# Text dataset can be scaled in terms of record size
textdataset_recordsize_factor=

# Two modes of generator: push,periodic
# Push means to send data to kafka cluster as fast as it could
# Periodic means sending data according to sending rate specification
mode=

# Under push mode: number of total records that will be generated
records=

# Following three params are under periodic mode
# Record count per interval
recordPerInterval=

# Interval time (in ms)
intervalSpan=

# Total round count of data send
totalRound=