#!/bin/bash 
bin=`dirname "$0"`
currpath=`cd "$bin"; pwd`

# read configure
. configure.sh

while read CHILD
do 
	echo "`date`:CHILD${CHILD} begin" 1>&2
	mkdir -p data/child${CHILD}

	#DBGEN_DATA=${DBGEN_DATA}
	
	# create data file for parallel tables
	for tp in $TABLES_PARALLEL ; do
	        ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -table ${tp} -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
		if [ "${tp}" != "web_sales" ]
		then
	        	hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${tp}_${CHILD}_${PARALLEL}.dat ${DBGEN_DATA}/${tp}/${tp}_${CHILD}_${PARALLEL}.dat &
   		else
			gzip data/child${CHILD}/${tp}_${CHILD}_${PARALLEL}.dat
        		hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${tp}_${CHILD}_${PARALLEL}.dat.gz ${DBGEN_DATA}/${tp}/${tp}_${CHILD}_${PARALLEL}.dat.gz &
		fi
	done
	
	# create data file for parallel refresh data
	./dsdgen -scale ${SCALE} -dir data/child${CHILD} -update 1 -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
	for rt in s_zip_to_gmt s_inventory s_web_order s_web_order_lineitem s_web_returns s_customer s_customer_address s_item s_promotion s_web_page s_web_site s_call_center s_warehouse
	do
		hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${rt}_${CHILD}_${PARALLEL}.dat ${DBGEN_DATA}/${rt}/${rt}_${CHILD}_${PARALLEL}.dat &
	done
	
	# create data file for no_parallel tables and refresh data
	if [ ${CHILD} -eq 1 ]
	then
		for rt in delete 
                do
                        hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${rt}_1.dat ${DBGEN_DATA}/${rt}/${rt}_1.dat &
                done

		for tnp in $TABLES_NO_PARALLEL ; do
		    ./dsdgen -scale ${SCALE} -dir data/child${CHILD} -table ${tnp} -terminate N 2>&1
		    hadoop fs -Ddfs.umask=0000 -moveFromLocal data/child${CHILD}/${tnp}.dat ${DBGEN_DATA}/${tnp}/${tnp}.dat &
		done
	fi
	wait

done 

cd $currpath
