#!/usr/bin/env bash

# exit when any command fails
set -e

export HADOOP_VERSION=3.1.0
export SPARK_VERSION=2.4.0
export SPARK_BIN_VERSION=spark2.4
export SPARK_HADOOP_VERSION=hadoop3.1
export HIVE_VERSION=3.0.0

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

# mvn build
mvn clean package -q -Dmaven.javadoc.skip=true -Dspark=2.4 -Dscala=2.11 -Dhive=3.0 -Dhadoop=3.1

# Setup cluster contain of hadoop and spark
source $GITHUB_WORKSPACE/actions/test-cluster/setup-cluster.sh

#Setup Hibench
source $GITHUB_WORKSPACE/actions/test-cluster/setup-hibench.sh

echo "========================================="
echo "Cluster Testing with Spark Version: $SPARK_VERSION"
echo "========================================="

# run all examples
source $GITHUB_WORKSPACE/bin/run_all.sh
