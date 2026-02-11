import yaml
import sys
import os
import copy


if __name__ == "__main__":
    assert(len(sys.argv)==3) # expects run_defs and list of ids
    assert(os.path.isfile(sys.argv[1]))
    with open(sys.argv[1]) as f:
        parsed_dict = yaml.safe_load(f)
    with open(sys.argv[2]) as f:
        id_and_counts = f.readlines()

    assert('system' in parsed_dict.keys() )
    assert('system' in parsed_dict.keys() )

    id_and_counts = [l.replace("\n","") for l in id_and_counts]

    new_run_definitions = []
    for line in id_and_counts:
        id, success, count = line.split(',')
        success, count = int(success),int(count)
        for r in parsed_dict['run_definitions']:
            if r['identifier'] == id:
                for i in range(5-success):
                    new_r = copy.deepcopy(r)
                    new_r['run'] = count+i
                    new_r['identifier'] = r['identifier'].replace(f"__{r['run']:03d}_", f"__{count+i:03d}_")
                    new_run_definitions.append(new_r)
                break
        else:
            raise ValueError(f"ID not found: '{id}'")


    parsed_dict['run_definitions'] = new_run_definitions
    ret_str = yaml.dump(parsed_dict, sort_keys=False, default_flow_style=False, indent=2)
    print(ret_str)


