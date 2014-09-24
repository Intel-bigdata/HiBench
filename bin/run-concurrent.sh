#!/bin/bash
DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. $DIR/bin/hibench-config.sh

START_TIME=`timestamp`
JOBS_NUM=0

if [ -f $HIBENCH_REPORT ]; then
    rm $HIBENCH_REPORT
fi

while read line
do
    if [[ $line == \#* ]]; then
        continue
    fi

    benchmark=`echo $line | awk '{print $1}'`
    numbers=`echo $line | awk '{print $2}'`

    if [ "$numbers" = "0" ]; then
        continue
    fi

    JOBS_NUM=$(( $JOBS_NUM + $numbers ))

    for i in $(seq $numbers)
    do
      if [ "$benchmark" = "dfsioe" ]; then
        echo "$benchmark is not supported"
      else
        echo "Running $benchmark in background"
        $DIR/$benchmark/bin/run.sh $i > /dev/null 2>&1 &
      fi
    done
done < $DIR/conf/benchmarks-concurrent.lst

for pid in $(jobs -p); do
  wait $pid
done

END_TIME=`timestamp`
DURATION=$(echo "scale=3;($END_TIME-$START_TIME)/1000"|bc)
echo `echo "scale=5;$JOBS_NUM/$DURATION"|bc`
