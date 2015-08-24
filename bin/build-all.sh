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
set -u

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

CURDIR=`pwd`
cd $DIR/src
mvn clean package      && \
cd $DIR/src/sparkbench && \
( mkdir jars | true )

for mr in MR1 MR2; do
        for spark_version in 1.2 1.3 1.4; do
                cp target/*-jar-with-dependencies.jar jars
                mvn clean package -D spark$spark_version -D $mr
                if [ $? -ne 0 ]; then
                        echo "Build failed for spark$spark_version and $mr, please check!"
                        exit 1
                fi
        done
done
cp jars/*.jar target/                       && \
rm -rf jars

result=$?
cd $CURDIR

if [ $result -ne 0 ]; then
    echo "Build failed, please check!"
else
    echo "Build all done!"
fi
