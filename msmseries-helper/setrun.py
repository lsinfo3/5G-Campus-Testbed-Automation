import yaml
import sys
import os


def change_run(d, n):
    old_run = d['run']
    d['run'] = n
    old_id = d['identifier']
    print(d['identifier'])
    print(f"__{old_run:03d}_")
    print(f"__{n:03d}_")
    d['identifier'] = old_id.replace(f"__{old_run:03d}_", f"__{n:03d}_")
    print(d['identifier'])
    return d


if __name__ == "__main__":
    assert(len(sys.argv)==3) # expects run_defs and number of run
    assert(os.path.isfile(sys.argv[1]))
    with open(sys.argv[1]) as f:
        parsed_dict = yaml.safe_load(f)
    new_run_nr = int(sys.argv[2])

    assert('system' in parsed_dict.keys() )

    for i,run in enumerate(parsed_dict['run_definitions']):
        old_id = run['identifier']
        parsed_dict['run_definitions'][i] = change_run(run, new_run_nr)
        print(parsed_dict['run_definitions'][i])
        assert(parsed_dict['run_definitions'][i]['identifier']!= old_id)

    ret_str = yaml.dump(parsed_dict, sort_keys=False, default_flow_style=False, indent=2)
    print(ret_str)


