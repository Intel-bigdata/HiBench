## Flink Properties ##

Please configure these properties in `conf/flink.conf`

    cp conf/flink.conf.template conf/flink.conf

Property      | Meaning
--------------|--------------------------
hibench.streambench.flink.home    |               /PATH/TO/YOUR/FLINK/HOME
hibench.flink.master               |              The master of Flink. HOSTNAME:PORT
hibench.streambench.flink.parallelism            | Default parallelism of flink job
hibench.streambench.flink.bufferTimeout          | The buffer timeout
hibench.streambench.flink.checkpointDuration     | The checkpoint duration

