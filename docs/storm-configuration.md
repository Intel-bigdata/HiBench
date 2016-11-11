## Storm Properties ##

Please configure these properties in `conf/storm.conf`

    cp conf/storm.conf.template conf/storm.conf

Property      | Meaning
--------------|--------------------------
hibench.streambench.storm.nimbus            |    host name of storm nimbus
hibench.streambench.storm.nimbusAPIPort     |    API port of storm nimbus
hibench.streambench.storm.home              |    /PATH/TO/YOUR/STORM/HOME
hibench.streambench.storm.worker_count      |     Number of workers. Number of most bolt threads is also equal to this param.
hibench.streambench.storm.spout_threads     |     Number of kafka spout threads of Storm
hibench.streambench.storm.bolt_threads      |     Number of bolt threads altogether
hibench.streambench.storm.localshuffle      |     Use local shuffle or not. Default true.
hibench.streambench.storm.nimbusContactInterval | Time interval to contact nimbus to judge if finished
hibench.streambench.storm.read_from_start       | Indicates whether to read data from kafka from the start or go on to read from last position
hibench.streambench.storm.ackon         | whether to turn on ack

