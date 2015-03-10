#!/bin/bash
#This script takes one argument scaleFactor
set -u
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
DIR=`cd $bin/../; pwd`

text_dataset="$DIR/src/main/resources/test1.data"
text_seed="$DIR/src/main/resources/test1seed.data"

if [ ! -f $text_seed ]; then
	echo "Usage: Require $text_seed file. Please re-download it first."
fi
seed_line_length=`head -2 $text_seed | tail -1 | wc -c`
#echo "Seed first record size:$seed_line_length"

scale_factor=1
if [ $# -gt 0 ]; then
	scale_factor=$1
fi

need_compute=true
if [ -f "$text_dataset" ];  then
	text_dataset_line_length=`head -2 $text_dataset | tail -1 | wc -c`
	if [ "$(($scale_factor*$seed_line_length))" == "$text_dataset_line_length"  ]; then
		need_compute=false
	fi
fi

if [ "$need_compute" == "true"  ]; then
	echo "==========start gen dataset=========="
	IFS=''
	> $text_dataset
	while read line
	do
		number=$scale_factor
		multiple_line=$line
		while [ $number -gt 1 ]
		do
			multiple_line="$multiple_line $line"
			number=$(($number-1))
		done
		echo $multiple_line >> $text_dataset
	done < $text_seed
else
	echo "==========dataset exists============="	
fi
