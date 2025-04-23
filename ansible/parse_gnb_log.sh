#!/bin/bash




LOGPATH="/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/578de3b8/578de3b8__0/gnb.log.gz"
GNBTYPE="srsRAN"


parse_srsran() {
    # INFO: this is for loglevel: 'info'
    echo -ne "TS,CQI,SNR,RSRP,MCS\n"
    gzip -dc "$LOGPATH" | grep "pusch" | grep -e '^[0-9]' | while read -r line; do
        ts="$(echo -n "$line" | awk '{print $1}')"
        ts="$(TZ=UTC date --date="$ts" "+%s.%N")"
        cqi="$(echo -n "$line" | awk '{print $10}' | cut -d "=" -f 2)"
        snr="$(echo -n "$line" | awk '{print $18}' | cut -d "=" -f 2)"
        rsrp="$(echo -n "$line" | awk '{print $19}' | cut -d "=" -f 2)"
        mcs="$(echo -n "$line" | awk '{print $20}' | cut -d "=" -f 2)"
        # 2025-04-17T20:29:22.556987 [METRICS ] [I] Scheduler UE Metrics: pci=1 rnti=4602 cqi=15 ri=1 dl_mcs=0 dl_brate_kbps=0 dl_nof_ok=0 dl_nof_nok=0 dl_error_rate=0% dl_bs=0 pusch_snr_db=n/a pusch_rsrp_db=n/a ul_mcs=0 ul_brate_kbps=0 ul_nof_ok=0 ul_nof_nok=0 ul_error_rate=0% bsr=0 last_ta=0s last_phr=30

        echo -ne "$ts,$cqi,$snr,$rsrp,$mcs\n"
    done
}

parse_oai() {
    # TODO: <
    true
}



if [[ $GNBTYPE == "srsRAN" ]]; then
    parse_srsran
fi

