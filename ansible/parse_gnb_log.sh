#!/bin/bash


BASEPATH="/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps"
BASEPATH="/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/"
BASEPATH="/home/lks/Akten/datastore/5g-masterarbeit/dockerization"
BASEPATH="/home/lks/Akten/datastore/5g-masterarbeit/gnb-versions-delay"
BASEPATH="/home/lks/Documents/datastore/5g-masterarbeit/throughput-overshoot"


# LOGPATH="/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/578de3b8/578de3b8__0/gnb.log.gz"
# GNBTYPE="srsRAN"
# LOGPATH="/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/578de3b8/578de3b8__cd2bd61d__001/gnb.log.gz"
# GNBTYPE="OAI"


# Very unlikely used single-byte character
DELIM=""


parse_srsran() {
    # INFO: this is for loglevel: 'info'
    echo -ne "TIMESTAMP,CQI,SNR,RSRP,MCS_DL,MCS_UL\n"
    gzip -dc "$LOGPATH" | grep "pusch" | grep -e '^[0-9]' | while read -r line; do
        ts="$(echo -n "$line" | awk '{print $1}')"
        ts="$(TZ=UTC date --date="$ts" "+%s.%N")"
        cqi="$(echo -n "$line" | sed "s/cqi=/$DELIM/" | cut -d "$DELIM" -f 2 | awk '{print $1}' | sed 's/n\/a//')"
        snr="$(echo -n "$line" | sed "s/pusch_snr_db=/$DELIM/" | cut -d "$DELIM" -f 2 | awk '{print $1}' | sed 's/n\/a//')"
        rsrp="$(echo -n "$line" | sed "s/pusch_rsrp_db=/$DELIM/" | cut -d "$DELIM" -f 2 | awk '{print $1}' | sed 's/n\/a//')"
        mcs_dl="$(echo -n "$line" | sed "s/dl_mcs=/$DELIM/" | cut -d "$DELIM" -f 2 | awk '{print $1}' | sed 's/n\/a//')"
        mcs_ul="$(echo -n "$line" | sed "s/ul_mcs=/$DELIM/" | cut -d "$DELIM" -f 2 | awk '{print $1}' | sed 's/n\/a//')"
        # 2025-04-17T20:29:22.556987 [METRICS ] [I] Scheduler UE Metrics: pci=1 rnti=4602 cqi=15 ri=1 dl_mcs=0 dl_brate_kbps=0 dl_nof_ok=0 dl_nof_nok=0 dl_error_rate=0% dl_bs=0 pusch_snr_db=n/a pusch_rsrp_db=n/a ul_mcs=0 ul_brate_kbps=0 ul_nof_ok=0 ul_nof_nok=0 ul_error_rate=0% bsr=0 last_ta=0s last_phr=30

        echo -ne "$ts,$cqi,$snr,$rsrp,$mcs_dl,$mcs_ul\n"
    done
}

parse_oai() {
    echo -ne "TIMESTAMP,CQI,SNR,RSRP,MCS_DL,MCS_UL\n"
    gzip -dc "$LOGPATH" | grep " RSRP \| MCS \| SNR " | grep -e '^[0-9]' | while read -r line; do
        ts="$(echo -n "$line" | awk '{print $1}')"
        cqi=""
        snr="$(echo -n "$line" | grep " SNR " | awk '{print $23}')"
        rsrp="$(echo -n "$line" | grep " RSRP " | awk '{print $16}')"
        mcs_dl="$(echo -n "$line" | grep " MCS " | grep -v "dlsch_rounds" | awk '{print $14}')"
        mcs_ul="$(echo -n "$line" | grep " MCS " | grep -v "ulsch_rounds" | awk '{print $14}')"

        # 1745504694.380505 UE RNTI e7eb CU-UE-ID 1 in-sync PH 52 dB PCMAX 21 dBm, average RSRP -70 (31 meas)
        # 1745504694.380582 UE e7eb: dlsch_rounds 22637/3/1/0, dlsch_errors 0, pucch0_DTX 3, BLER 0.00000 MCS (1) 9
        # 1745504694.380614 UE e7eb: ulsch_rounds 77550/23000/219/0, ulsch_errors 0, ulsch_DTX 209, BLER 0.06702 MCS (1) 8 (Qm 4 deltaMCS 0 dB) NPRB 5  SNR 14.0 dB
        echo -ne "$ts,$cqi,$snr,$rsrp,$mcs_dl,$mcs_ul\n"
    done
}



walk_dir() {
    for run_group in "$BASEPATH"/*/; do
        for run in "$run_group"/*/; do
            run_name="$(basename "$run")"
            gnb_type="$(jq -r '.gnb_version.type' < "$run/$run_name.yaml")"
            echo "$run  --- $gnb_type "
            LOGPATH="$run/gnb.log.gz"

            if [[ $gnb_type == "srsRAN" ]]; then
                continue
                parse_srsran > "$run"/gnb_snr.csv
            elif [[ $gnb_type == "OAI" ]]; then
                parse_oai > "$run"/gnb_snr.csv
            fi
        done
    done
}



time walk_dir



