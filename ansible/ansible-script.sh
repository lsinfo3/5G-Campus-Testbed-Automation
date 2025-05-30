#!/bin/bash


# 10. Start playbook, run until exit
# 11. If exit == 0 -> done, else -> 20.
# 20. reboot pc until interface present
# 30. get task_id of last successfully completed run
# 31. if not present, start from beginning, else -> 40.
# 40. start ansible playbook with `skip_until_id=<last_id>` -> 11.

# TODO: iperf server
# TODO: scapy server x2


ansible_log="/home/lks/Documents/datastore/5g-masterarbeit/throughput-overshoot-scapy/ansible_task.log"
# Ansible_Playbook_Extra_Vars="skip_until_id=4343fe5c__94c63a77__001"
Ansible_Playbook_Extra_Vars="skip_until_id=4343fe5c__9ba06c22__000"
# Ansible_Playbook_Extra_Vars=""




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

start_scapy_server_on_gnodeb() {
    ssh gnodeb sudo python3 udp-server.py -i 0.0.0.0 -p 3344 -a false &
}

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
    if [[ unsuccessfull_reboots -ge 5 ]]; then
        echo "Can't find interface after reboot" 1>&2
        gotify "playbook aborted" "$(( ($(date +%s)- $Start_Date) /60 ))min, Exit after $unsuccessfull_reboots consecutive reboots without finding the interface."
        exit 43
    fi
    start_scapy_server_on_gnodeb &
}

build_ansible_playbook_extra_vars() {
    if [ ! -f "$ansible_log" ]; then
        echo ""
        gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Ansible log has not been found."
    fi
    last_id=$(cat "$ansible_log" | tail -n 1 | awk '{print $4}')
    if [ "$last_id" == "" ]; then
        echo ""
        gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Empty task read."
    else
        echo "skip_until_id=$last_id"
        gotify "playbook status" "$(( ($(date +%s)- $Start_Date) /60 ))min, Continuing after task: '$last_id'."
    fi
}




start_scapy_server_on_gnodeb &
while true; do
    ansible-playbook playbooks/traffic-gen.yaml --extra-vars "$Ansible_Playbook_Extra_Vars"
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


