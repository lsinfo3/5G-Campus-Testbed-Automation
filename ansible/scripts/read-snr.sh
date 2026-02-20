#!/bin/bash



DEV_PATH="/dev/cdc-wdm0"

CSV_DEST="$1"


echo "TIMESTAMP,SNR,RSRP,RSRQ,SINR" > "$CSV_DEST"

CID_01="$(qmicli -d "$DEV_PATH" --nas-noop --client-no-release-cid | tail -n 1 | awk '{print $2}' | tr -d "'")"
CID_02="$(qmicli -d "$DEV_PATH" --nas-noop --client-no-release-cid | tail -n 1 | awk '{print $2}' | tr -d "'")"

while true; do
    sleep 0.1
    QUERY="$(qmicli -d "$DEV_PATH" --nas-get-signal-info --client-no-release-cid --client-cid="$CID_01" 2>/dev/null)"
    if ! echo "$QUERY" | grep "Successfully got signal info" >/dev/null 2>&1; then
        echo failure
        continue
    fi
    SNR=$(echo "$QUERY" | grep "SNR" | awk '{print $2}' | tr -d "'")
    RSRP=$(echo "$QUERY" | grep "RSRP" | awk '{print $2}' | tr -d "'")
    RSRQ=$(echo "$QUERY" | grep "RSRQ" | awk '{print $2}' | tr -d "'")
    SINR=""
    TIMESTAMP="$(date "+%s.%3N")"
    echo "$TIMESTAMP,$SNR,$RSRP,$RSRQ,$SINR" >> "$CSV_DEST"

    sleep 0.1
    SNR=""
    RSRP=""
    RSRQ=""
    SINR="$(qmicli -d "$DEV_PATH" --nas-get-signal-strength --client-no-release-cid --client-cid="$CID_02" | grep "SINR" | awk '{print $3}' | tr -d "'")"
    TIMESTAMP="$(date "+%s.%3N")"
    echo "$TIMESTAMP,$SNR,$RSRP,$RSRQ,$SINR" >> "$CSV_DEST"
done
