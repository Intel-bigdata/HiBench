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
( mkdir jars | true )  && \
cp target/*-jar-with-dependencies.jar jars  && \
mvn clean package -D spark1.2               && \
cp target/*-jar-with-dependencies.jar jars  && \
mvn clean package -D MR1                    && \
cp target/*-jar-with-dependencies.jar jars  && \
mvn clean package -D MR1 -D spark1.2        && \
cp jars/*.jar target/                       && \
rm -rf jars

result=$?
cd $CURDIR

if [ $result -ne 0 ]; then
    echo "Build failed, please check!"
else
    echo "Build all done!"
fi
