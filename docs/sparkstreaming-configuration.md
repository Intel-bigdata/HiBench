## Spark Streaming Properties ##

Please configure these properties in `conf/spark.conf`

    cp conf/spark.conf.template conf/spark.conf

Property      | Meaning
--------------|--------------------------
hibench.streambench.spark.batchInterval       | Spark streaming Batchnterval in millisecond (default 100)
hibench.streambench.spark.receiverNumber      | Number of nodes that will receive Kafka input (default: 4)
hibench.streambench.spark.storageLevel  | Indicate RDD storage level. Indicate RDD storage level. (default: 2) 0 = StorageLevel.MEMORY_ONLY, 1 = StorageLevel.MEMORY_AND_DISK_SER, other = StorageLevel.MEMORY_AND_DISK_SER_2
hibench.streambench.spark.enableWAL  | Indicate whether to test the write ahead log new feature (default: false)
hibench.streambench.spark.checkpointPath | If testWAL is true, this path is to store stream context in hdfs shall be specified. If false, it can be empty (default: /var/tmp)
hibench.streambench.spark.useDirectMode | Whether to use direct approach or not (dafault: true)
