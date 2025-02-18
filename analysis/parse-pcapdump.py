import packetparser as pp
import os
import time
import multiprocessing as mp

""" Read ansible pcap dump. Parse pcaps and extract relevant data. Write csvs back """


ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps/"


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
        return pp.parse_pcap_gtp(infile=infile, outfile=outfile+".csv.gz", udpport=DEFAULT_PORT)
    elif infile.endswith("ue.pcap") or infile.endswith("ue.pcap.gz"):
        return pp.parse_pcap_ip(infile=infile, outfile=outfile+".csv.gz", udpport=DEFAULT_PORT)
    else:
        raise ValueError(f"Wrong pcap format: {infile}")



# print(test_configurations)
# print(runs)
# print(pcaps)

start = time.time()
with mp.Pool(8) as p:
    for log in p.imap_unordered(pp_wrapper,get_pcap_paths()):
        print(log)

print(f"Took {time.time()-start}s")




