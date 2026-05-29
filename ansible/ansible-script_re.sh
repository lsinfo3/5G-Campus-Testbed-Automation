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


export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"
PY312=/usr/bin/python3.12



Ansible_Playbook_Extra_Vars=""
Continue_Playbook=false
Rerun_Failed=false
Rerun_Failed_Attempts=0
Recover_Incomplete=false
Recover_Incomplete_Attempts=0




Start_Date=$(date +%s);

Reboots=0

gotify() {
    local gotify_bin

    gotify_bin="$(type -P gotify)"
    if [[ -n "$gotify_bin" ]]; then
        "$gotify_bin" "$@"
    fi
}


wait_for_ssh() {
    SSHHOST="${@: -1}"
    while ! ssh -o ConnectTimeout=5s -o RemoteCommand=none $SSHHOST exit; do
        sleep 5
    done
}

usage() {
    echo "Usage: $0 [--continue] [--rerun-failed N | --recover-incomplete N] <test-series-definition>" >&2
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --continue)
                Continue_Playbook=true
                shift
                ;;
            --rerun-failed)
                if [[ $# -lt 2 ]] || [[ ! "$2" =~ ^[0-9]+$ ]]; then
                    usage
                    echo "--rerun-failed requires a non-negative integer argument." >&2
                    exit 1
                fi
                Rerun_Failed=true
                Rerun_Failed_Attempts="$2"
                shift 2
                ;;
            --recover-incomplete)
                if [[ $# -lt 2 ]] || [[ ! "$2" =~ ^[0-9]+$ ]]; then
                    usage
                    echo "--recover-incomplete requires a non-negative integer argument." >&2
                    exit 1
                fi
                Recover_Incomplete=true
                Recover_Incomplete_Attempts="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            --*)
                usage
                echo "Unknown option: $1" >&2
                exit 1
                ;;
            *)
                if [[ -n "$TEST_SERIES_DEFINIITON" ]]; then
                    usage
                    echo "Only one test series definition can be provided." >&2
                    exit 1
                fi
                TEST_SERIES_DEFINIITON="$1"
                shift
                ;;
        esac
    done

    if [[ -z "$TEST_SERIES_DEFINIITON" ]]; then
        usage
        exit 1
    fi

    if [[ "$Rerun_Failed" == true ]] && [[ "$Recover_Incomplete" == true ]]; then
        usage
        echo "--rerun-failed and --recover-incomplete cannot be used together." >&2
        exit 1
    fi
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

validate_test_series_definition() {
    if [[ -f "$TEST_SERIES_DEFINIITON" ]] && \
        cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq -e 'has("system")' && \
        cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq -e '.system | has("pcap_dump")'; then
        TEST_SERIES_PCAPDUMP="$( cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq -r '.system.pcap_dump' )"
    else
        echo "Provided file must be existent and provide full definition!" >&2
        exit 1
    fi
}

print_scheduled_measurements() {
    NR_OF_TASKS="$(cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq '.run_definitions[].identifier' | wc -l)"
    NR_OF_IDs="$(cat "$TEST_SERIES_DEFINIITON" | yaml2json | jq '.run_definitions[].identifier' | sort | uniq | wc -l)"
    if [[ $NR_OF_TASKS -ne $NR_OF_IDs ]]; then
        echo "Scheduled $NR_OF_TASKS measurements but only $NR_OF_IDs unique IDs!!"
        exit 1
    else
        echo "Scheduled $NR_OF_TASKS measurements."
    fi
}

run_ansible_playbook() {
    local test_series_definition="$1"
    local ansible_playbook_extra_vars="$2"

    while true; do
        ansible-playbook playbooks/measurements.yaml --extra-vars "@${test_series_definition}" --extra-vars "$ansible_playbook_extra_vars"
        ansible_return_code=$?
        if [ $ansible_return_code -eq 0 ]; then
            break
        fi
        echo -e "\nAnsible failed! \nTo quit, press ctrl+c again, otherwise gnb will reboot and measurements will continue.\n"
        sleep 10
        handle_reboot
        ansible_playbook_extra_vars="$(build_ansible_playbook_extra_vars)"
    done
}

get_run_ids_from_definition() {
    cat "$1" | yaml2json | jq -r '.run_definitions[].identifier'
}

resolve_failed_run_id() {
    local result_dir_name="$1"
    local stripped_run_id

    if [[ -n "${valid_run_ids[$result_dir_name]+x}" ]]; then
        echo "$result_dir_name"
        return 0
    fi

    stripped_run_id="$(printf '%s\n' "$result_dir_name" | sed -E 's/__[0-9]+_[[:alnum:]]+$//')"
    if [[ "$stripped_run_id" != "$result_dir_name" ]] && [[ -n "${valid_run_ids[$stripped_run_id]+x}" ]]; then
        echo "$stripped_run_id"
        return 0
    fi

    return 1
}

collect_failed_run_ids() {
    local results_dir="$1"
    local test_series_definition="$2"
    local failed_file
    local result_dir_name
    local run_id
    declare -A valid_run_ids=()
    declare -A failed_run_ids=()

    FAILED_RUN_IDS=()

    while IFS= read -r run_id; do
        valid_run_ids["$run_id"]=1
    done < <(get_run_ids_from_definition "$test_series_definition")

    if [[ ! -d "$results_dir" ]]; then
        return 0
    fi

    while IFS= read -r -d '' failed_file; do
        result_dir_name="$(basename "$(dirname "$failed_file")")"
        if run_id="$(resolve_failed_run_id "$result_dir_name")"; then
            failed_run_ids["$run_id"]=1
        fi
    done < <(find "$results_dir" -type f -name FAILED -print0)

    if [[ ${#failed_run_ids[@]} -eq 0 ]]; then
        return 0
    fi

    mapfile -t FAILED_RUN_IDS < <(printf '%s\n' "${!failed_run_ids[@]}" | sort)
}

collect_incomplete_run_ids() {
    local results_dir="$1"
    local test_series_definition="$2"
    local result_dir
    local result_dir_name
    local failed_file
    local run_id
    declare -A valid_run_ids=()
    declare -A existing_result_run_ids=()
    declare -A incomplete_run_ids=()

    INCOMPLETE_RUN_IDS=()

    while IFS= read -r run_id; do
        valid_run_ids["$run_id"]=1
    done < <(get_run_ids_from_definition "$test_series_definition")

    if [[ -d "$results_dir" ]]; then
        while IFS= read -r -d '' result_dir; do
            result_dir_name="$(basename "$result_dir")"
            if run_id="$(resolve_failed_run_id "$result_dir_name")"; then
                existing_result_run_ids["$run_id"]=1
            fi
        done < <(find "$results_dir" -type d -print0)

        while IFS= read -r -d '' failed_file; do
            result_dir_name="$(basename "$(dirname "$failed_file")")"
            if run_id="$(resolve_failed_run_id "$result_dir_name")"; then
                incomplete_run_ids["$run_id"]=1
            fi
        done < <(find "$results_dir" -type f -name FAILED -print0)
    fi

    for run_id in "${!valid_run_ids[@]}"; do
        if [[ -z "${existing_result_run_ids[$run_id]+x}" ]]; then
            incomplete_run_ids["$run_id"]=1
        fi
    done

    if [[ ${#incomplete_run_ids[@]} -eq 0 ]]; then
        return 0
    fi

    mapfile -t INCOMPLETE_RUN_IDS < <(printf '%s\n' "${!incomplete_run_ids[@]}" | sort)
}

print_failed_runs() {
    echo "Failed runs:"
    printf '  - %s\n' "$@"
}

print_incomplete_runs() {
    echo "Incomplete runs:"
    printf '  - %s\n' "$@"
}

remove_failed_result_dirs_for_run_ids() {
    local results_dir="$1"
    shift
    local failed_file
    local result_dir
    local result_dir_name
    local run_id
    local remove_return_code=0
    declare -A valid_run_ids=()
    declare -A selected_run_ids=()
    declare -A selected_result_dirs=()

    for run_id in "$@"; do
        valid_run_ids["$run_id"]=1
        selected_run_ids["$run_id"]=1
    done

    while IFS= read -r -d '' failed_file; do
        result_dir="$(dirname "$failed_file")"
        result_dir_name="$(basename "$result_dir")"
        if run_id="$(resolve_failed_run_id "$result_dir_name")" && [[ -n "${selected_run_ids[$run_id]+x}" ]]; then
            selected_result_dirs["$result_dir"]=1
        fi
    done < <(find "$results_dir" -type f -name FAILED -print0)

    for result_dir in "${!selected_result_dirs[@]}"; do
        rm -rf -- "$result_dir" || remove_return_code=1
    done

    return $remove_return_code
}

write_filtered_test_series_definition() {
    local source_definition="$1"
    local target_definition="$2"
    shift 2

    python - "$source_definition" "$target_definition" "$@" <<'PY'
import sys
import yaml

source_definition = sys.argv[1]
target_definition = sys.argv[2]
selected_ids = set(sys.argv[3:])

with open(source_definition) as source:
    definition = yaml.safe_load(source)

run_definitions = definition.get("run_definitions", [])
definition["run_definitions"] = [
    run_definition
    for run_definition in run_definitions
    if str(run_definition.get("identifier")) in selected_ids
]

with open(target_definition, "w") as target:
    yaml.safe_dump(definition, target, sort_keys=False)
PY
}

rerun_failed_runs() {
    local attempt=0
    local temp_definition
    local collect_return_code

    collect_failed_run_ids "$TEST_SERIES_PCAPDUMP" "$TEST_SERIES_DEFINIITON"
    collect_return_code=$?
    if [[ $collect_return_code -ne 0 ]]; then
        exit $collect_return_code
    fi

    while [[ ${#FAILED_RUN_IDS[@]} -gt 0 ]] && [[ $attempt -lt $Rerun_Failed_Attempts ]]; do
        attempt=$((attempt+1))
        echo "Rerunning failed runs ($attempt/$Rerun_Failed_Attempts):"
        print_failed_runs "${FAILED_RUN_IDS[@]}"

        remove_failed_result_dirs_for_run_ids "$TEST_SERIES_PCAPDUMP" "${FAILED_RUN_IDS[@]}" || exit 46

        temp_definition="$(mktemp)"
        write_filtered_test_series_definition "$TEST_SERIES_DEFINIITON" "$temp_definition" "${FAILED_RUN_IDS[@]}"
        run_ansible_playbook "$temp_definition" ""
        rm -f -- "$temp_definition"

        collect_failed_run_ids "$TEST_SERIES_PCAPDUMP" "$TEST_SERIES_DEFINIITON"
        collect_return_code=$?
        if [[ $collect_return_code -ne 0 ]]; then
            exit $collect_return_code
        fi
    done

    if [[ ${#FAILED_RUN_IDS[@]} -gt 0 ]]; then
        print_failed_runs "${FAILED_RUN_IDS[@]}"
        exit 44
    fi
}

recover_incomplete_runs() {
    local attempt=0
    local temp_definition
    local collect_return_code

    collect_incomplete_run_ids "$TEST_SERIES_PCAPDUMP" "$TEST_SERIES_DEFINIITON"
    collect_return_code=$?
    if [[ $collect_return_code -ne 0 ]]; then
        exit $collect_return_code
    fi

    while [[ ${#INCOMPLETE_RUN_IDS[@]} -gt 0 ]] && [[ $attempt -lt $Recover_Incomplete_Attempts ]]; do
        attempt=$((attempt+1))
        echo "Recovering incomplete runs ($attempt/$Recover_Incomplete_Attempts):"
        print_incomplete_runs "${INCOMPLETE_RUN_IDS[@]}"

        remove_failed_result_dirs_for_run_ids "$TEST_SERIES_PCAPDUMP" "${INCOMPLETE_RUN_IDS[@]}" || exit 46

        temp_definition="$(mktemp)"
        write_filtered_test_series_definition "$TEST_SERIES_DEFINIITON" "$temp_definition" "${INCOMPLETE_RUN_IDS[@]}"
        run_ansible_playbook "$temp_definition" ""
        rm -f -- "$temp_definition"

        collect_incomplete_run_ids "$TEST_SERIES_PCAPDUMP" "$TEST_SERIES_DEFINIITON"
        collect_return_code=$?
        if [[ $collect_return_code -ne 0 ]]; then
            exit $collect_return_code
        fi
    done

    if [[ ${#INCOMPLETE_RUN_IDS[@]} -gt 0 ]]; then
        print_incomplete_runs "${INCOMPLETE_RUN_IDS[@]}"
        exit 44
    fi
}


parse_args "$@"
validate_test_series_definition
print_scheduled_measurements

gotify "playbook start" "Starting new playbook for $TEST_SERIES_PCAPDUMP."

ansible_log="$TEST_SERIES_PCAPDUMP/measurements.log"

if [[ "$Continue_Playbook" == true ]]; then
    Ansible_Playbook_Extra_Vars="$(build_ansible_playbook_extra_vars)"
fi




# If FAILED markers already exist, this is a recovery invocation: rerun only
# those failed measurements instead of repeating the whole test series first.
if [[ "$Rerun_Failed" == true ]]; then
    collect_failed_run_ids "$TEST_SERIES_PCAPDUMP" "$TEST_SERIES_DEFINIITON"
    collect_return_code=$?
    if [[ $collect_return_code -ne 0 ]]; then
        exit $collect_return_code
    fi
fi

# start_scapy_server_on_gnodeb &
if [[ "$Recover_Incomplete" == true ]]; then
    recover_incomplete_runs
elif [[ "$Rerun_Failed" == true ]] && [[ ${#FAILED_RUN_IDS[@]} -gt 0 ]]; then
    rerun_failed_runs
else
    run_ansible_playbook "$TEST_SERIES_DEFINIITON" "$Ansible_Playbook_Extra_Vars"

    if [[ "$Rerun_Failed" == true ]]; then
        rerun_failed_runs
    fi
fi

gotify "playbook done" "$(( ($(date +%s)- $Start_Date) /60 ))min, fully completed after $Reboots reboots."
