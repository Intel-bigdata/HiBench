
### Build All ###
To simply build all modules in Hibench, use the below command. This could be time consuming because the hadoopbench relies on 3rd party tools like Mahout and Nutch. The build process automatilly downloads these tools for you. If you won't run these workloads, you can only build a specific framework to speed up the build process.

    mvn clean package
 

### Build a specific framework benchmark ###
HiBench 6.0 supports building only bechmarks for a specific framework. For example, to build the hadoop benchmarks only, we can use the below command:

    mvn -Phadoopbench clean package

To build hadoop and spark benchmarks

    mvn -Phadoopbench -Psparkbench clean package

Supported frameworks includs: hadoopbench, sparkbench, flinkbench, stormbench, gearpumpbench.

### Specify Spark Version ###
To specify the spark version, use -Dspark=xxx(1.6 or 2.0). By default, it builds for spark 1.6.   

    mvn -Psparkbench -Dspark=2.0 clean package

### Build a single module ###
If you are only interested in a single workload in HiBench. You can build a single module. For example, the below command only builds the sql workloads for Spark.

    mvn -Psparkbench -Dmodules -Psql clean package

Supported modules includes: micro, ml(machine learning), sql, websearch, graph, streaming.
