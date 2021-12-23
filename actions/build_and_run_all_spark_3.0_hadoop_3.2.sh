#!/usr/bin/env bash

# exit when any command fails
set -e

export HADOOP_VERSION=3.2.1
export SPARK_VERSION=3.0.0
export SPARK_BIN_VERSION=spark3.0
export SPARK_HADOOP_VERSION=hadoop3.2
export HIVE_VERSION=3.0.0

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

# mvn build
mvn clean package -q -Dmaven.javadoc.skip=true -Dspark=3.0 -Dscala=2.12

# Setup cluster contain of hadoop and spark
source $GITHUB_WORKSPACE/actions/test-cluster/setup-cluster.sh

#Setup Hibench
source $GITHUB_WORKSPACE/actions/test-cluster/setup-hibench.sh

echo "========================================="
echo "Cluster Testing with Spark Version: $SPARK_VERSION"
echo "========================================="

# run all examples
source $GITHUB_WORKSPACE/bin/run_all.sh