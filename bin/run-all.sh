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

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`
DICT_PATH=/usr/share/dict/words

if [ -n "$1" ]; then
	DICT_PATH="$2"
fi

. $DIR/bin/hibench-config.sh "-dict" "$DICT_PATH"

if [ -f $HIBENCH_REPORT ]; then
    rm $HIBENCH_REPORT
fi

for benchmark in `cat $DIR/conf/benchmarks.lst`; do
    if [[ $benchmark == \#* ]]; then
        continue
    fi

    if [ "$benchmark" = "dfsioe" ] ; then
        # dfsioe specific
        $DIR/dfsioe/bin/prepare-read.sh
        $DIR/dfsioe/bin/run-read.sh
        $DIR/dfsioe/bin/run-write.sh

    elif [ "$benchmark" = "hivebench" ]; then
        # hivebench specific
        $DIR/hivebench/bin/prepare.sh
        $DIR/hivebench/bin/run-aggregation.sh
        $DIR/hivebench/bin/run-join.sh

    else
        if [ -e $DIR/${benchmark}/bin/prepare.sh ]; then
            $DIR/${benchmark}/bin/prepare.sh
        fi
        $DIR/${benchmark}/bin/run.sh
    fi
done

