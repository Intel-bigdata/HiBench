#!/bin/bash

#modify etc/hosts file to avoid connection error
cp /etc/hosts /etc/hostsbak
sed -i 's/::1/#::1/' /etc/hostsbak
cat /etc/hostsbak > /etc/hosts
rm -rf /etc/hostsbak
#run wordcount example
/usr/bin/restart-hadoop-spark.sh
${HIBENCH_HOME}/bin/workloads/micro/wordcount/prepare/prepare.sh
${HIBENCH_HOME}/bin/workloads/micro/wordcount/hadoop/run.sh
