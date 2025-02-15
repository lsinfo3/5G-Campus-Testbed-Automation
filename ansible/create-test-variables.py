import yaml
import argparse
import hashlib

yaml.Dumper.ignore_aliases = lambda *args : True # Don't reference identical types


GLOBAL_COUNTER = 0

system = {
    "pcap_dump": "../dumps/",
    "identifier":0
    }

fixed_params = {
    "distance_horizontal_in_m": 0.5,
    "distance_vertical_in_m": 0.35,
    "gnb_antenna_inclanation_in_degree": 90,
    "gnb_antenna_rotation_in_degree": 0,
    "ue_antenna_inclanation_in_degree": 90,
    "ue_antenna_rotation_in_degree": 0,
    "modem": "SIM8200EA-M2 5G HAT",         # TODO: Verify!
    "interface_ue": "wwan0",
    "interface_gnb": "eno1"
    }

run_to_run_params_default = {
    "identifier" : 0,
    "run": 1,
    "traffic_config": {                     # TODO: build_traffic_config function
        "traffic_type" : "ping",
        "traffic_duration": 60,
        "icmp_intervall": 0.01,
        "icmp_count": 6000
    },
    "tdd_config": {
        "tdd_dl_ul_ratio": 2,
        "tdd_flex_slots": 1,
        "tdd_dl_ul_tx_period": 10,
        "tdd_dl_slots": 6,
        "tdd_dl_symbols": 8,
        "tdd_ul_slots": 3,
        "tdd_ul_symbols": 0,
        }
    }





def dict_to_small_hash(d: dict) -> str:
    d_str = yaml.dump(d)
    return hashlib.sha256(d_str.encode()).hexdigest()[:8]

""" extra_flex_slots are used when a ratio of 1 is set but there should be flex slots """
def build_tdd_config(period=10, ratio=2, dl_symbols = 8, ul_symbols = 4, min_flex_slots = 0):
    slots = (period) / (ratio + 1)
    flex_slots = int(period - (ratio+1)*int(slots))
    # if min_flex_slots > 0 and flex_slots == 0:
    #     slots = int((period-min_flex_slots) // (ratio + 1))
    #     flex_slots = int(period - (ratio+1)*slots)


    if min_flex_slots == 0 and flex_slots == 0 and dl_symbols != 0:
        # TODO: logger warnung!
        dl_symbols = 0

    dl_slots = int(ratio*slots)
    ul_slots = int(slots)

    if min_flex_slots == 1:
        if flex_slots == 0 and ratio == 1:
            ul_slots -= 1
            dl_symbols = 5
            ul_symbols = 7
        elif flex_slots == 0:
            dl_slots -= 1
            dl_symbols = 12
            ul_symbols = 0
    elif min_flex_slots > 1:
        raise ValueError(f"Only 0 or 1 min_flex_slots are supported, but got {min_flex_slots}!")

    tdd_config = {
        "tdd_dl_ul_ratio": ratio,
        "tdd_flex_slots": flex_slots,
        "tdd_dl_ul_tx_period": period,
        "tdd_dl_slots": dl_slots,
        "tdd_dl_symbols": dl_symbols,
        "tdd_ul_slots": ul_slots,
        "tdd_ul_symbols": ul_symbols,
        }
    return tdd_config

def new_per_run_config_base():
    # r = run_to_run_params_default.copy()
    r = dict(run_to_run_params_default) # Deep copy via constructor
    global GLOBAL_COUNTER
    r["identifier"] = f"{dict_to_small_hash(fixed_params)}__{GLOBAL_COUNTER}"
    GLOBAL_COUNTER+=1
    return r



""" Returns list of all per run configurations """
def create_param_combinations():
    c = []

    for ratio in [1, 2, 4]:
        for period_length in [5, 10, 20]:
            r = new_per_run_config_base()
            r["tdd_config"] = build_tdd_config(period=period_length, ratio=ratio, min_flex_slots=1)
            c.append(r)

    return c





if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="create test configuration",
        description="Create .yaml file which contains all variables need for ansible"
            )
    parser.add_argument("filename")
    args = parser.parse_args()
    system["identifier"] = dict_to_small_hash(fixed_params)
    system["fixed_params"] = fixed_params
    d = {"system":system, "run_to_run_variation":create_param_combinations()}
    run_config_str = yaml.dump(d, sort_keys=False, indent=4)
    print(run_config_str)

    with open(args.filename, "w") as f:
        f.write(run_config_str)

