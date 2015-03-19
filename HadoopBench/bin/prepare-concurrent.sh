#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. $DIR/bin/hibench-config.sh

if [ -f $HIBENCH_REPORT ]; then
    rm $HIBENCH_REPORT
fi

cat $DIR/conf/benchmarks-concurrent.lst | while read line
do
    if [[ $line == \#* ]]; then
        continue
    fi

    benchmark=`echo $line | awk '{print $1}'`
    numbers=`echo $line | awk '{print $2}'`

    if [ "$numbers" = "0" ]; then
        continue
    fi

    if [ "$benchmark" != "dfsioe" ]; then
        if [ -e $DIR/$benchmark/bin/prepare.sh ]; then
            $DIR/$benchmark/bin/prepare.sh
        fi
    fi

done
