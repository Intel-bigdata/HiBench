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

. $DIR/bin/sparkbench-config.sh

if [ -f $SPARKBENCH_REPORT ]; then
    rm $SPARKBENCH_REPORT
fi

for benchmark in `cat $DIR/conf/benchmarks.lst`; do
    if [[ $benchmark == \#* ]]; then
        continue
    fi

    if [ -e $DIR/${benchmark}/prepare/prepare.sh ]; then
	echo "====================="
	echo "Prepare for ${benchmark}"
	echo "====================="
        $DIR/${benchmark}/prepare/prepare.sh
	result=$?
	if [ $result -ne 0 ]
	then
	    echo "ERROR: ${benchmark} failed to prepare successfully." 
	    exit $result
        fi
    fi
    
    # clear hive metastore
    find . -name "metastore_db" -exec "rm -rf {}" \; 2> /dev/null || true 
done

echo "Prepare all done!"
