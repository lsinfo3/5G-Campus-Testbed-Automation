import yaml
import sys
import os


if __name__ == "__main__":
    assert(len(sys.argv)>=3)
    assert(all([os.path.isfile(f) for f in sys.argv[1:]]))
    files = [ open(f) for f in sys.argv[1:] ]
    parsed_dicts = [ yaml.safe_load(f) for f in files ]
    assert(all([ 'system' in d.keys() for d in parsed_dicts ]))
    assert(all([ d['system'] == parsed_dicts[0]['system'] for d in parsed_dicts ])) # same system

    return_dict = parsed_dicts.pop(0)
    known_ids = [ r['identifier'] for r in return_dict['run_definitions'] ]
    for d in parsed_dicts:
        # first check there are no 2 runs with the same id!
        new_ids = [ r['identifier'] for r in d['run_definitions'] ]
        assert(len(known_ids)+len(new_ids) == len(set(known_ids+new_ids)))
        return_dict['run_definitions'].extend( d['run_definitions'] )

    ret_str = yaml.dump(return_dict, sort_keys=False, default_flow_style=False, indent=2)
    print(ret_str)


