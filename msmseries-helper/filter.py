import yaml
import sys
import os


if __name__ == "__main__":
    assert(len(sys.argv)==3) # expects run_defs and list of ids
    assert(os.path.isfile(sys.argv[1]))
    with open(sys.argv[1]) as f:
        parsed_dict = yaml.safe_load(f)
    with open(sys.argv[2]) as f:
        ids = f.readlines()

    assert('system' in parsed_dict.keys() )
    assert('system' in parsed_dict.keys() )

    ids = [l.replace("\n","") for l in ids]

    parsed_dict['run_definitions'] = [ r for r in parsed_dict['run_definitions'] if r['identifier'] in ids ]
    ret_str = yaml.dump(parsed_dict, sort_keys=False, default_flow_style=False, indent=2)
    print(ret_str)


