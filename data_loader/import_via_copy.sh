#!/bin/bash

if [ $# -ne 2 ]; then
    echo $0: usage: import.sh key_space tableName
    exit 1
fi

key_space=$1
table_name=$2
files_=`ls ./*.csv`
for file_ in $files_
do
	echo Processing file $file_
	cqlsh -e "use $key_space;copy \"$table_name\" from '%f' with HEADER='true';" localhost
done
