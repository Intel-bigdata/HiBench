############### common functions ################
function timestamp(){
    sec=`date +%s`
    nanosec=`date +%N`
    tmp=`expr $sec \* 1000 `
    msec=`expr $nanosec / 1000000 `
    echo `expr $tmp + $msec`
}

function configure(){
    local dir=$1

    # global configure
    . ${dir}/../configure.sh
    if [ $? != 0 ] ;then
        echo "Please make sure ${dir}/../configure.sh executes correctly."
        exit 1;
    fi

    # bench configure
    . ${dir}/configure.sh
    if [ $? != 0 ] ;then
        echo "Please make sure ${dir}/configure.sh executes correctly."
        exit 1;
    fi
}

function gen_report() {
    local type=$1
    local start=$2
    local end=$3
    local size=$4

    local duration=`echo "scale=6;($end-$start)/1000"|bc`
    local tput=`echo "scale=6;$size/$duration"|bc`

    echo "$type `date +%T` <$start,$end> $size $duration $tput"
}
