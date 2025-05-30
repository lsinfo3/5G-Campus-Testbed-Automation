



# extra_vars="--extra-vars=\"extra=test\""
extra_vars="extra=test"
extra_vars=""



ansible-playbook ./playbooks/script-test.yaml --extra-vars "$extra_vars"
