#!/bin/bash


# 10. Start playbook, run until exit
# 11. If exit == 0 -> done, else -> 20.
# 20. reboot pc until interface present
# 30. get task_id of last successfully completed run
# 31. if not present, start from beginning, else -> 40.
# 40. start ansible playbook with `skip_until_id=<last_id>` -> 11.


# TODO: iperf server
# TODO: scapy server x2
#
# TODO: get 'Ansible_Playbook_Extra_Vars' from log if requested




Ansible_Playbook_Extra_Vars=""




Start_Date=$(date +%s);

Reboots=0


wait_for_ssh() {
    SSHHOST="${@: -1}"
    while ! ssh -o ConnectTimeout=5s -o RemoteCommand=none $SSHHOST exit; do
        sleep 5
    done
}

reboot_and_check_interface() {
    gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Rebooting because of failed playbook."
    ssh gnodeb sudo reboot; wait_for_ssh gnodeb; ssh gnodeb ip a | grep "wwan0:"
    Reboots=$((Reboots+1))
}

# start_scapy_server_on_gnodeb() {
#     ssh gnodeb sudo python3 udp-server.py -i 0.0.0.0 -p 3344 -a true &
# }

handle_reboot() {
    reboot_and_check_interface
    reboot_return_code=$?
    unsuccessfull_reboots=1
    while [[ reboot_return_code -ne 0 ]] && [[ unsuccessfull_reboots -lt 5 ]]; do
        reboot_and_check_interface
        reboot_return_code=$?
        if [[ reboot_return_code -ne 0 ]]; then
            unsuccessfull_reboots=$((unsuccessfull_reboots+1))
        fi
    done
    if [[ unsuccessfull_reboots -ge 7 ]]; then
        echo "Can't find interface after reboot" 1>&2
        gotify "playbook aborted" "$(( ($(date +%s)- $Start_Date) /60 ))min, Exit after $unsuccessfull_reboots consecutive reboots without finding the interface."
        exit 43
    fi
    # start_scapy_server_on_gnodeb &
}

build_ansible_playbook_extra_vars() {
    if [ ! -f "$ansible_log" ]; then
        echo ""
        gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Ansible log has not been found."
    fi
    # awk selects specific columns, then prints last field
    last_id=$(cat "$ansible_log" | awk '$2 == "COMPLETED"' | tail -n 1 | awk '{print $NF}')
    if [ "$last_id" == "" ]; then
        echo ""
        gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Empty task read."
    else
        echo "skip_until_id=$last_id"
        gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Continuing after task: '$last_id'."
    fi
}



yaml2json() {
    python -c 'import sys,yaml,json; print(json.dumps(yaml.safe_load(str(sys.stdin.read())), sort_keys=False, indent=4))'
}


NR_OF_TASKS="$(cat "$1" | yaml2json | jq '.run_definitions[].identifier' | wc -l)"
NR_OF_IDs="$(cat "$1" | yaml2json | jq '.run_definitions[].identifier' | sort | uniq | wc -l)"
if [[ $NR_OF_TASKS -ne $NR_OF_IDs ]]; then
    echo "Scheduled $NR_OF_TASKS measurements but only $NR_OF_IDs unique IDs!!"
    exit 1
else
    echo "Scheduled $NR_OF_TASKS measurements."
fi





if [[ $# -ne 1 ]] && [[ $# -ne 2 ]]; then
    echo "Requires path to test series definition and optionally the '--continue' flag!" >&2
    exit 1
fi
if [[ $# -eq 2 ]] && [[ $1 != "--continue" ]]; then
    echo "Requires path to test series definition and optionally the '--continue' flag!" >&2
    exit 1
fi

gotify "playbook start" "Starting new playbook for $TEST_SERIES_PCAPDUMP."

# $# == 2 ==> '--continue'
TEST_SERIES_DEFINIITON="$1"
if [[ $# -eq 2 ]]; then
    TEST_SERIES_DEFINIITON="$2"
fi

# TODO: jq gibt aktuell noch "true" zurück
if [[ -f "$TEST_SERIES_DEFINIITON" ]] && \
    cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq -e 'has("system")' && \
    cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq -e '.system | has("pcap_dump")'; then
    TEST_SERIES_PCAPDUMP="$( cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq -r '.system.pcap_dump' )"
else
    echo "Provided file must be existent and provide full definition!" >&2
    exit 1
fi
ansible_log="$TEST_SERIES_PCAPDUMP/measurements.log"

# $# == 2 ==> '--continue'
if [[ $# -eq 2 ]]; then
    Ansible_Playbook_Extra_Vars="$(build_ansible_playbook_extra_vars)"
fi




# start_scapy_server_on_gnodeb &
while true; do
    ansible-playbook playbooks/measurements.yaml --extra-vars "@${TEST_SERIES_DEFINIITON}" --extra-vars "$Ansible_Playbook_Extra_Vars"
    ansible_return_code=$?
    if [ $ansible_return_code -eq 0 ]; then
        break
    fi
    echo -e "\nAnsible failed! \nTo quit, press ctrl+c again, otherwise gnb will reboot and measurements will continue.\n"
    sleep 10
    handle_reboot
    Ansible_Playbook_Extra_Vars="$(build_ansible_playbook_extra_vars)"
done

gotify "playbook done" "$(( ($(date +%s)- $Start_Date) /60 ))min, fully completed after $Reboots reboots."

