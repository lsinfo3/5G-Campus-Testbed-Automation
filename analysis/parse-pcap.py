import packetparser as pp
import os
import time
import multiprocessing as mp
import yaml
import argparse

""" Read ansible pcap dump. Parse pcaps and extract relevant data. Write csvs back """


# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/"


def get_pcap_paths(root: str):
    test_configurations = [e.path for e in os.scandir(root) if e.is_dir()]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]
    return pcaps



iperf_udp_throughput = pp.validator(proto="udp", pl_min_len=1200, seq_num_first_byte=8, seq_num_last_byte=12, match_port=4455)
scapy_ping_small = pp.validator(proto="udp", pl_min_len=8, pl_max_len=8, seq_num_first_byte=4, seq_num_last_byte=8, match_port=3344)
scapy_ping_big = pp.validator(proto="udp", pl_min_len=1300, seq_num_first_byte=4, seq_num_last_byte=8, match_port=3344)
# scapy_throughput = pp.validator(proto="udp", pl_min_len=1454, seq_num_first_byte=4, seq_num_last_byte=8, match_port=3344)
# scapy_throughput = pp.validator(proto="udp", pl_min_len=1428, seq_num_first_byte=4, seq_num_last_byte=8, match_port=3344)
scapy_throughput = pp.validator(proto="udp", pl_min_len=1300, seq_num_first_byte=4, seq_num_last_byte=8, match_port=3344)


def pp_wrapper(infile:str):

    dirname = os.path.basename(os.path.dirname(infile))
    run_config_path = f"{os.path.dirname(infile)}/{dirname}.yaml"

    with open(run_config_path, "r") as rf:
        run_config = yaml.safe_load(rf)

    if run_config["traffic_config"]["traffic_type"] == "scapyudpping":
        if run_config["traffic_config"]["size"] == "big":
            v = scapy_ping_big
        elif run_config["traffic_config"]["size"] == "small":
            v = scapy_ping_small
        else:
            raise ValueError("Unknown traffic packet size")
        v.match_port = run_config["traffic_config"]["target_port"]
    elif run_config["traffic_config"]["traffic_type"] == "scapyudpthroughput":
        v = scapy_throughput
        v.match_port = run_config["traffic_config"]["target_port"]
        v.proto = run_config["traffic_config"]["proto"]
    elif run_config["traffic_config"]["traffic_type"] == "iperfthroughput":
        v = iperf_udp_throughput
        v.match_port = run_config["traffic_config"]["target_port"]
        v.proto = run_config["traffic_config"]["proto"]
    else:
        raise RuntimeError("Unknown test specification!")


    # if not "110219ae__0" in infile:
    #     return f"skipped: {infile}"
    outfile = infile
    if outfile.endswith(".gz"):
        outfile = outfile[:-3]
    outfile = outfile[:-5] # strip .pcap

    if infile.endswith("gnb.pcap") or infile.endswith("gnb.pcap.gz"):
        return pp.parse_pcap_gtp(infile=infile, outfile=outfile+".csv.gz", validator=v)
    elif infile.endswith("ue.pcap") or infile.endswith("ue.pcap.gz"):
        return pp.parse_pcap_ip(infile=infile, outfile=outfile+".csv.gz", validator=v)
    else:
        raise ValueError(f"Wrong pcap format: {infile}")



# print(test_configurations)
# print(runs)
# print(pcaps)

def main():
    start = time.time()

    # p = get_pcap_paths(ansible_dump)[0]
    # print(pp_wrapper(p))

    with mp.Pool(8) as p:
        with open(f"{ansible_dump}/parse_pcap.log", "w") as f:
            for log in p.imap_unordered(pp_wrapper,get_pcap_paths(ansible_dump)):
                f.write(log+"\n")
                print(log)

    print(f"Took {time.time()-start}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="parse pcap recordings to csv",
        description="Scan given dir and"
            )
    parser.add_argument("filename")
    args = parser.parse_args()
    ansible_dump = args.filename
    main()







