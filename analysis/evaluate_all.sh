#!/bin/bash


# Parses all runs, optionally skip existing ones


SKIP_EXISTING=false

OPTS=$(getopt -o "" -l skip-existing -- "$@")
if [ $? -ne 0 ]; then
    exit 1
fi

eval set -- "$OPTS"

while true; do
    case "$1" in
        --skip-existing)
            SKIP_EXISTING=true
            shift
            ;;
        --)
            shift
            break
            ;;
    esac
done

# Required argument
if [ $# -lt 1 ] || [ $# -gt 1 ]; then
    echo "Usage: $0 [--skip-existing] <directory>"
    exit 1
fi

DIR="$1"



# Use this to indent inner function calls and easily differenciate stdout
run_indented() {
    "$@" \
      > >(sed 's/^/   ▎/') \
      2> >(sed 's/^/   ▎/' >&2)
}


echo "Directory: $DIR"
echo -e "Skip existing: $SKIP_EXISTING \n"


date
if [ $SKIP_EXISTING == "true" ]; then
    time run_indented python ./parse-pcap.py --skip "$1" || exit 1
else
    time run_indented python ./parse-pcap.py "$1" || exit 1
fi
date
echo -e "Successfuly parsed pcaps\n\n\n"
sleep 1


date
if [ $SKIP_EXISTING == "true" ]; then
    export SKIP_EXISTING
fi
time run_indented bash ./parse-gnblog.sh "$1" || exit 2
date
echo -e "Successfuly parsed gnb logs\n\n\n"
sleep 1


date
if [ $SKIP_EXISTING == "true" ]; then
    time run_indented python ./parse-csvs.py --skip "$1" || exit 3
else
    time run_indented python ./parse-csvs.py "$1" || exit 3
fi
date
echo -e "Successfuly parsed csvs\n\n\n"


# date
# time python ./parse-mcs.py  "$1" || exit 1
# date
# echo -e "\n\n\nSuccessfuly split mcs"
# sleep 1

