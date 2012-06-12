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

#!/bin/bash

DIR=`dirname "$0"`
if [ -f $HIBENCH_REPORT ]; then
    rm $HIBENCH_REPORT
fi

for benchmark in `cat $DIR/benchmarks.lst`; do
    if [[ $benchmark == \#* ]]; then
        continue
    fi

    if [ "$benchmark" = "dfsioe" ] ; then
        # dfsioe specific
        bash $DIR/dfsioe/prepare-read.sh
        bash $DIR/dfsioe/run-read.sh
        bash $DIR/dfsioe/run-write.sh

    elif [ "$benchmark" = "hivebench" ]; then
        # hivebench specific
	bash $DIR/hivebench/prepare.sh
	bash $DIR/hivebench/run-aggregation.sh
	bash $DIR/hivebench/run-join.sh

    else
        if [ -e $DIR/${benchmark}/prepare.sh ]; then
            bash $DIR/${benchmark}/prepare.sh
        fi
        bash $DIR/${benchmark}/run.sh
    fi
done
  
