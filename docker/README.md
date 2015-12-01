# HiBench Docker Project


This is the docker image for [intel-hibench](https://github.com/intel-hadoop/HiBench) implementation.
We build a  pseudo single-node environment on Ubuntu 14.04.
Before building/running, please check the file
```bash
   $./hibench-docker.conf
```
for configuration and please set the versions according to your own needs.
Otherwise, it will progress by default.


### Build and Run Container with HiBench and all dependencies installed

Launch the container by executing:

```bash
    $./scripts/run-container.sh cdh
```
OR
```bash
    $./scripts/run-container.sh open-source
```

The script will launch all related services and run a workload (by default: wordcount) after initialization.
When entering the container of the hibench-docker, you are normally guided to a root user, please modify configurations under the HiBench directory :
```bash
    #cd ${HIBENCH_HOME}
```

After running any workload, please check this file
```bash
   ${HIBENCH_HOME}/report/hibench.report
```
for detailed report.

If there is anything wrong with a certain workload, please check this final configuration file:
```bash
   ${HIBENCH_HOME}/report/${workload}/${method}/conf/${workload}.conf
```
Remember to replace the workload variable with the actual one (e.g. wordcount) and the method variable with one of ( mapreduce, spark/scala, spark/java, spark/python ).
