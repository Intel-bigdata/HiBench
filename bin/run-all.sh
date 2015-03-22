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


for benchmark in `cat $DIR/conf/benchmarks.lst`; do
    if [[ $benchmark == \#* ]]; then
        continue
    fi

    # clear hive metastore
    find $DIR -name "metastore_db" -exec rm -rf "{}" \; 2> /dev/null || true

    echo "====================="
    echo "Prepare ${benchmark} ..."
    echo "====================="
    
    WORKLOAD=$DIR/workloads/${benchmark}
    echo "${WORKLOAD}/prepare/prepare.sh"
    "${WORKLOAD}/prepare/prepare.sh"

    if [ $? -ne 0 ]
    then
	echo "ERROR: ${benchmark} prepare failed!" 
        continue
    fi

    for target in `cat $DIR/conf/languages.lst`; do
	if [[ $target == \#* ]]; then 
	    continue
	fi
	echo "====================="
	echo "Run ${benchmark}/${target}"
	echo "====================="
	echo $WORKLOAD/${target}/bin/run.sh
	$WORKLOAD/${target}/bin/run.sh

	result=$?
	if [ $result -ne 0 ]
	then
	    echo "ERROR: ${benchmark}/${target} failed to run successfully." 
            exit $result
	fi
    done
done

echo "Run all done!"
