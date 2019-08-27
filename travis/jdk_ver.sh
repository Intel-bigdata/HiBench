#!/bin/bash
java_cmd="$JAVA_HOME/bin/java"
IFS=$'\n'
lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
for line in $lines; do
    if [[ (-z $result) && ($line = *"version \""*) ]]
    then
    ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
    if [[ $ver = "1."* ]]
    then
        result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
    else
        result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
    fi
    fi
done

echo "$result"
