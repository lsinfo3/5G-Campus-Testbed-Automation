import yaml
import copy
import argparse
import hashlib

yaml.Dumper.ignore_aliases = lambda *args : True # Don't reference identical types


# OAI--v2.1.0--9fab2124417cfe67fe09b1eab5e377e26c5cf3a5
# OAI--v2.2.0--68191088ab4cdcd47d6c0764ac5cf2483f4b3d29
# SRS--release_24_04--c33cacba7d940e734ac7bad08935cbc35578fad9
# SRS--release_24_10--9d5dd742a70e82c0813c34f57982f9507f1b6d5d




GLOBAL_COUNTER = 0

system = {
    "pcap_dump": "/home/lks/Documents/datastore/5g-masterarbeit/gnb-versions-delay/",
    "identifier":0
    }

# TODO:  Should different types of traffic generation result in the same identifier=hash?
fixed_params = {
    "distance_horizontal_in_m": 0.5,
    "distance_vertical_in_m": 0.0,
    "gnb_antenna_inclanation_in_degree": 90,
    "gnb_antenna_rotation_in_degree": 0,
    "ue_antenna_inclanation_in_degree": 90,
    "ue_antenna_rotation_in_degree": 0,
    "modem": "SIM8200EA-M2 5G HAT",         # TODO: Verify!
    "interface_ue": "wwan0",
    "interface_gnb": "eno1",
    "jammer": False,                         # TODO: more options?
    "sdr": "B210",
    "performance_tuning": False             # set governor, drm kms polling, pinning?, etc
    }

run_to_run_params_default = {
    "identifier" : 0,
    "run": 0,
    "dockerization" : False,
    "gnb_version": {
        "type": "srsRAN",
        "uhd_version": "UHD-4.0",
        "version": "",
        "commit":""
        #TODO: "version": xyz
        },
    "traffic_config": {                     # TODO: build_traffic_config function
        "traffic_type" : "scapyudpping",    # 'scapyudpping', 'iperfthroughput' or 'ping'
        "direction": "Ul",                   # only relevant for 'iperfthroughput'
        "traffic_duration": 60,
        "proto": "udp",
        "dist": "det",
        "iat": "0.01",
        "rate": "100M",                     # only relevant for throughput measurements
        "size": "small",
        "count": "6000",
        "target_ip": "10.45.0.1",
        "target_port": "3344",
        "burst": "1",
    },
    "rx_gain": None,                        # Good values depend on the environment,
    "tx_gain": None,                        # determin beforehand and set in gnb config defaults
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
    r = copy.deepcopy(run_to_run_params_default)
    # global GLOBAL_COUNTER
    # r["identifier"] = f"{dict_to_small_hash(fixed_params)}__{GLOBAL_COUNTER}"
    # GLOBAL_COUNTER+=1
    return r



""" Returns list of all per run configurations """
def create_param_combinations():
    c = []

    ratios = [1,2,4]
    ratios = [1,2]
    # ratios = [2]

    period_lengths = [5, 10, 20]
    period_lengths = [5, 20]
    # period_lengths = [20]

    runs = [ i for i in range(3) ]

    srsranconfs = [
            {"type":"srsRAN","uhd_version": "UHD-3.15.LTS", "version": "release_24_04", "commit":"c33cacba7d940e734ac7bad08935cbc35578fad9"},
            {"type":"srsRAN","uhd_version": "UHD-3.15.LTS", "version": "release_24_10", "commit":"9d5dd742a70e82c0813c34f57982f9507f1b6d5d"},
            {"type":"srsRAN","uhd_version": "UHD-4.0", "version": "release_24_04", "commit":"c33cacba7d940e734ac7bad08935cbc35578fad9"},
            {"type":"srsRAN","uhd_version": "UHD-4.0", "version": "release_24_10", "commit":"9d5dd742a70e82c0813c34f57982f9507f1b6d5d"},
            ]
    oairanconfs = [
            {"type":"OAI","uhd_version": "UHD-3.15.LTS", "version": "v2.1.0", "commit":"9fab2124417cfe67fe09b1eab5e377e26c5cf3a5"},
            {"type":"OAI","uhd_version": "UHD-3.15.LTS", "version": "v2.2.0", "commit":"68191088ab4cdcd47d6c0764ac5cf2483f4b3d29"},
            {"type":"OAI","uhd_version": "UHD-4.0", "version": "v2.1.0", "commit":"9fab2124417cfe67fe09b1eab5e377e26c5cf3a5"},
            {"type":"OAI","uhd_version": "UHD-4.0", "version": "v2.2.0", "commit":"68191088ab4cdcd47d6c0764ac5cf2483f4b3d29"},
            ]

    for gnb in srsranconfs + oairanconfs:
        # run_to_run_params_default["traffic_config"]["scapy_iat"]="0.01"
        # run_to_run_params_default["traffic_config"]["scapy_count"]="6000"
        # for ratio in ratios:
        #     for period_length in period_lengths:
        #         r = new_per_run_config_base()
        #         r["tdd_config"] = build_tdd_config(period=period_length, ratio=ratio, min_flex_slots=1)
        #         r["gnb_version"] = gnb
        #         c.append(r)


        # iperf
        if False:
            run_to_run_params_default["traffic_config"]["traffic_type"] = "iperfthroughput"
            run_to_run_params_default["traffic_config"]["target_port"] = "4455"
            run_to_run_params_default["traffic_config"]["iat"]="0.001"
            run_to_run_params_default["traffic_config"]["count"]="60000"
            for dockerization in [True, False]:
                for direction in ["Dl", "Ul"]:
                    run_to_run_params_default["traffic_config"]["direction"]=direction
                    for ratio in ratios:
                        for period_length in period_lengths:
                            for run in runs:
                                r = new_per_run_config_base()
                                r["tdd_config"] = build_tdd_config(period=period_length, ratio=ratio, min_flex_slots=1)
                                r["gnb_version"] = gnb
                                r["dockerization"] = dockerization
                                r["run"] = 0 # hash shouldn't change between runs
                                r["identifier"] = dict_to_small_hash(fixed_params) + "__" + dict_to_small_hash(r) + f"__{run:03d}"
                                # r["identifier"] = r["identifier"] + f"__{run:03d}"
                                r["run"] = run
                                c.append(r)


        # # scapy
        if True:
            run_to_run_params_default["traffic_config"]["traffic_type"] = "scapyudpping"
            run_to_run_params_default["traffic_config"]["target_port"] = "3344"
            run_to_run_params_default["traffic_config"]["iat"]="0.001"
            run_to_run_params_default["traffic_config"]["count"]="60000"
            run_to_run_params_default["traffic_config"]["direction"]="UlDl"
            for ratio in ratios:
                for period_length in period_lengths:
                    for run in runs:
                        r = new_per_run_config_base()
                        r["tdd_config"] = build_tdd_config(period=period_length, ratio=ratio, min_flex_slots=1)
                        r["gnb_version"] = gnb
                        r["run"] = 0
                        r["identifier"] = dict_to_small_hash(fixed_params) + "__" + dict_to_small_hash(r) + f"__{run:03d}"
                        # r["identifier"] = r["identifier"] + f"__{run:03d}"
                        r["run"] = run
                        c.append(r)
    return c

def create_antenna_gain_combinations():
    c = []
    ratios = [1]
    period_lengths = [5]

    runs = [ i for i in range(3) ]

    OAI_RX_MIN = 20
    OAI_TX_MIN = 40
    SRS_RX_MIN = 0
    SRS_TX_MIN = 30

    OAI_RX_MAX_RANGE = [0, 70]
    OAI_TX_MAX_RANGE = [0, 89]
    OAI_RX_RANGE = [i for i in range(OAI_RX_MIN, 70, 4)]
    OAI_TX_RANGE = [i for i in range(OAI_TX_MIN, 89, 4)]

    SRS_RX_MAX_RANGE = [0, 76]
    SRS_TX_MAX_RANGE = [0, 89]
    SRS_RX_RANGE = [i for i in range(SRS_RX_MIN, 70, 4)]
    SRS_TX_RANGE = [i for i in range(SRS_TX_MIN, 89, 4)]


    srsranconfs = [
            # {"type":"srsRAN","uhd_version": "UHD-3.15.LTS", "version": "release_24_04", "commit":"c33cacba7d940e734ac7bad08935cbc35578fad9"},
            # {"type":"srsRAN","uhd_version": "UHD-3.15.LTS", "version": "release_24_10", "commit":"9d5dd742a70e82c0813c34f57982f9507f1b6d5d"},
            # {"type":"srsRAN","uhd_version": "UHD-4.0", "version": "release_24_04", "commit":"c33cacba7d940e734ac7bad08935cbc35578fad9"},
            {"type":"srsRAN","uhd_version": "UHD-4.0", "version": "release_24_10", "commit":"9d5dd742a70e82c0813c34f57982f9507f1b6d5d"},
            ]
    oairanconfs = [
            # {"type":"OAI","uhd_version": "UHD-3.15.LTS", "version": "v2.1.0", "commit":"9fab2124417cfe67fe09b1eab5e377e26c5cf3a5"},
            # {"type":"OAI","uhd_version": "UHD-3.15.LTS", "version": "v2.2.0", "commit":"68191088ab4cdcd47d6c0764ac5cf2483f4b3d29"},
            # {"type":"OAI","uhd_version": "UHD-4.0", "version": "v2.1.0", "commit":"9fab2124417cfe67fe09b1eab5e377e26c5cf3a5"},
            {"type":"OAI","uhd_version": "UHD-4.0", "version": "v2.2.0", "commit":"68191088ab4cdcd47d6c0764ac5cf2483f4b3d29"},
            ]

    for gnb in srsranconfs + oairanconfs:
        for rxtx in ["rx", "tx"]:
            # iperf
            run_to_run_params_default["traffic_config"]["traffic_type"] = "iperfthroughput"
            run_to_run_params_default["traffic_config"]["target_port"] = "4455"
            run_to_run_params_default["traffic_config"]["iat"]="0.001"
            run_to_run_params_default["traffic_config"]["count"]="60000"
            run_to_run_params_default["traffic_config"]["direction"]="Dl"
            for ratio in ratios:
                for period_length in period_lengths:
                    if rxtx == "rx":
                        if gnb["type"] == "srsRAN":
                            gain_range = SRS_RX_RANGE
                        elif gnb["type"] == "OAI":
                            gain_range = OAI_RX_RANGE
                        else:
                            raise RuntimeError()
                    elif rxtx == "tx":
                        if gnb["type"] == "srsRAN":
                            gain_range = SRS_TX_RANGE
                        elif gnb["type"] == "OAI":
                            gain_range = OAI_TX_RANGE
                        else:
                            raise RuntimeError()
                    else:
                        raise RuntimeError()
                    for gain in gain_range:
                        for run in runs:
                            r = new_per_run_config_base()
                            r["tdd_config"] = build_tdd_config(period=period_length, ratio=ratio, min_flex_slots=1)
                            if rxtx == "rx":
                                r["rx_gain"] = gain
                                format_id = f"rx-{gain:02d}"
                            elif rxtx == "tx":
                                r["tx_gain"] = gain
                                format_id = f"tx-{gain:02d}"
                            else:
                                raise RuntimeError()
                            r["gnb_version"] = gnb
                            r["run"] = 0
                            r["identifier"] = dict_to_small_hash(fixed_params) + "__" + gnb["type"] + "__" + format_id + f"__{run:03d}"
                            r["run"] = run
                            c.append(r)
    return c






if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="create test configuration",
        description="Create .yaml file which contains all variables need for ansible"
            )
    parser.add_argument("--findgain", dest="findgain", action="store_true")
    parser.set_defaults(findgain=False)
    parser.add_argument("filename")
    args = parser.parse_args()

    system["identifier"] = dict_to_small_hash(fixed_params)
    system["fixed_params"] = fixed_params
    if args.findgain:
        d = {"system":system, "run_to_run_variation":create_antenna_gain_combinations()}
        run_config_str = yaml.dump(d, sort_keys=False, indent=4)
        print(run_config_str)
    else:
        d = {"system":system, "run_to_run_variation":create_param_combinations()}
        run_config_str = yaml.dump(d, sort_keys=False, indent=4)
        print(run_config_str)

    with open(args.filename, "w") as f:
        f.write(run_config_str)

