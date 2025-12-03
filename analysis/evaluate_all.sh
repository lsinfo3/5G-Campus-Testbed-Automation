#!/bin/bash

if [[ $# -ne 1 ]]; then
    echo "Expected path to measurement data"
    exit 1
fi


date
time python ./parse-pcap.py "$1" || exit 1
date
echo -e "\n\n\n Successfuly parsed pcaps"
sleep 5


date
time bash ./parse-gnblog.sh "$1"  || exit 2
date
echo -e "\n\n\n Successfuly parsed gnb logs"
sleep 5


date
time python ./parse-csvs.py "$1" || exit 3
date
echo -e "\n\n\n Successfuly parsed csvs"
sleep 5


date
time python ./parse-parsed.py "$1" || exit 4
date
echo -e "\n\n\n Success."
sleep 5
