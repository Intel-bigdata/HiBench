#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


CUR_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
HOME_DIR=${CUR_DIR}/..

CONFIG_FILE=${HOME_DIR}/hibench-docker.conf
DOCKERFILE_ADDR=${HOME_DIR}/base/Dockerfile
OPENSOURCE_HADOOP_ENV_FILE=${HOME_DIR}/opensource-docker/scripts/hadoop-env.sh
CDH_HADOOP_ENV_FILE=${HOME_DIR}/cdh-docker/scripts/hadoop-env.sh
CLOUDERAFILE_ADDR=${HOME_DIR}/cdh-docker/conf/cloudera.list
echo "# build environment on ubuntu14.04" > ${DOCKERFILE_ADDR}
echo "# Packages for Cloudera's Distribution for Hadoop, Spark" > ${CLOUDERAFILE_ADDR}

OS_DETAIL="FROM "
while IFS='' read -r line || [[ -n "$line" ]]; do
    case $line in 
      \#*)
        echo "$line" >> ${DOCKERFILE_ADDR}
        ;;
      "")
        echo "$line" >> ${DOCKERFILE_ADDR}
        ;;
      OS_DIST*)
        OS_DIST=`echo $line | cut -d ' ' -f 2`
        OS_DETAIL=${OS_DETAIL}${OS_DIST}
        ;;
      OS_VERSION*)
        OS_VERSION=`echo $line | cut -d ' ' -f 2`
        OS_DETAIL=${OS_DETAIL}:${OS_VERSION}
        echo ${OS_DETAIL} >> ${DOCKERFILE_ADDR}
        ;;
      CDH_VERSION_DETAIL*)
        CDH_VERSION_DETAIL=`echo $line | cut -d ' ' -f 2`
        CDH_VERSION=`echo $CDH_VERSION_DETAIL | cut -d '.' -f 1`
        echo "deb [arch=amd64] http://archive.cloudera.com/cdh${CDH_VERSION}/ubuntu/trusty/amd64/cdh trusty-cdh${CDH_VERSION_DETAIL} contrib" >> ${CLOUDERAFILE_ADDR}
        echo "deb-src http://archive.cloudera.com/cdh${CDH_VERSION}/ubuntu/trusty/amd64/cdh trusty-cdh${CDH_VERSION_DETAIL} contrib" >> ${CLOUDERAFILE_ADDR}
        ;;
      
    JDK*)
        echo "ENV $line"  >> ${DOCKERFILE_ADDR}

        JDK_VERSION=`echo $line | cut -d ' ' -f 2`
        sed -i 's/java-[0-9]/java-'"$JDK_VERSION"'/' $CDH_HADOOP_ENV_FILE
        sed -i 's/java-[0-9]/java-'"$JDK_VERSION"'/' $OPENSOURCE_HADOOP_ENV_FILE
	;;
      *)
        echo "ENV $line"  >> ${DOCKERFILE_ADDR}
        ;;
    esac
done < "$CONFIG_FILE"

cat ${HOME_DIR}/base/base-core >> ${DOCKERFILE_ADDR}
