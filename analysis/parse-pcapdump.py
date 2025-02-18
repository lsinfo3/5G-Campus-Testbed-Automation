import packetparser as pp
import os
import multiprocessing as mp

""" Read ansible pcap dump. Parse pcaps and extract relevant data. Write csvs back """


ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/"


def get_pcap_paths():
    test_configurations = [e.path for e in os.scandir(ansible_dump) if e.is_dir()]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]
    return pcaps


# DEFAULT_PORT=2152
DEFAULT_PORT=3344

def pp_wrapper(infile:str):
    outfile = infile
    if outfile.endswith(".gz"):
        outfile = outfile[:-3]
    outfile = outfile[:-5] # strip .pcap
    if infile.endswith("gnb.pcap") or infile.endswith("gnb.pcap.gz"):
        pp.parse_pcap_gtp(infile=infile, outfile=outfile+".csv", udpport=DEFAULT_PORT)
    elif infile.endswith("ue.pcap") or infile.endswith("ue.pcap.gz"):
        pp.parse_pcap_ip(infile=infile, outfile=outfile+".csv", udpport=DEFAULT_PORT)
    else:
        raise ValueError(f"Wrong pcap format: {infile}")





# print(test_configurations)
# print(runs)
# print(pcaps)

with mp.Pool(8) as p:
    p.map(pp_wrapper,get_pcap_paths())




