# 5G Testbed


## Preparation

On the core, for scapy measurements, start the `ansible/scripts/udp-server.py`, i.e. ` sudo ./udp-server.py -i 0.0.0.0 -p 3344 -a true`.

For iperf3 measurements, the iperf server must be started on the core `sudo iperf3 -s -p 4455 --bind 0.0.0.0`.


## Measurement Procedure

1. Create a measurement series definition, `ansible/create-test-variables.py` can be used for that.

1. Add the path for the series definition to `ansible/playbooks/traffic-gen.yaml` to the task 'include test configuration'

1. If automatic reboots (i.e. after modem failure) should be suported, don't start the `traffic-gen` playbook directly but use the `ansible/ansible-script.sh` wrapper. The path to the ansible.log must be edited in the script if the `ansible-script.sh` is used. The directory is the same as in the YAML measurement definition.

1. After the measurements have been completed, the following scripts can be used to analyse the results. `<measurement-path>` is the path from the YAML measurement definition.
    1. `analysis/parse-pcap.py <measurement-path>` parses the pcaps and drops all unimportant information. CSVs are created for each pcap.
    1. `analysis/parse-csvs.py <measurement-path>` parses the CSVs and aggreagtes delay and throughput on a per-run basis. Provides an aggregated `.csv.gz`/`.parquet` in the measurement path.
    1. `analysis/parse-gnblog.sh <measurement-path>` parses the gNB logs and extracts SNR,MCS and other channel metrics.



## OAI
2.4 91e7030c
2.3 8bf6d5d7    2025.06.04
2.2 68191088    2024.11.22


699afafc 2025.02.27   min/max dl/ul mcs support added



# TODOS

## bandwidth requires ARFCN values for OAI -- how to incorporate?

valid ARFCN values for OAI depend on the bandwidth, how do we incorporate these settings?

New yaml with 'bandwidth-definitions'? Srsran: only center-freq, OAI: AbsolutePointA,...

should also include used bandwidth? i.e. "name of bandwidth config" (+ gnb_type,version,other req. params) -> "dict including bandwidth and and arfcn values"

## move definition of usb devices to config

## core pinning

## autostart of iperf & udp server

## warning if min/max mcs is not supported because of the gnb version

## move tinkerforge, powercycles, etc into own modules






