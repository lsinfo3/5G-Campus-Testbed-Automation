import argparse
import traceback
import dpkt
import socket
import time
import gzip
import os
import json
import binascii
import numpy as np

import dataclasses

# TODO: handle ipv6 in all branches
# TODO: combine gtp and non gtp parser?


DEBUG=False


# TODO: define expectation of inner packet: udp/tcp? ports?
@dataclasses.dataclass()
class validator:
    # INFO: payload in question is the inner (udp/tcp) payload
    proto: str | None = None
    match_port: int | None = None           # One of src/dst port has to have this port number
    sport: int | None = None
    dport: int | None = None
    pl_min_len: int | None = None
    pl_max_len: int | None = None
    seq_num_first_byte: int | None = None   # inclusive
    seq_num_last_byte: int | None = None    # exclusive

    def validate(self, ip_pkt: dpkt.ip.IP)-> bool:
        try:
            if self.proto:
                if self.proto.casefold() == "udp":
                    assert(isinstance(ip_pkt.data, dpkt.udp.UDP))
                if self.proto.casefold() == "tcp":
                    assert(isinstance(ip_pkt.data, dpkt.tcp.TCP))
            if self.sport:
                assert(ip_pkt.data.sport == self.sport)
            if self.dport:
                assert(ip_pkt.data.dport == self.dport)
            if self.pl_min_len:
                assert(len(ip_pkt.data.data) >= self.pl_min_len)
            if self.pl_max_len:
                assert(len(ip_pkt.data.data) <= self.pl_max_len)

            return True

        except AssertionError as ae:
            # print(ae.with_traceback())
            if DEBUG:
                traceback.print_exception(ae)
            return False

empty_validator = validator()


iperf_udp_throughput = validator(proto="udp", pl_min_len=1200, seq_num_first_byte=8, seq_num_last_byte=12)
scapy_ping = validator(proto="udp", pl_min_len=8, pl_max_len=8, seq_num_first_byte=4, seq_num_last_byte=8)



def parse_pcap_gtp(infile, outfile, validator: validator=empty_validator):
    t0 = time.time()
    pktid = 0
    icmps = {
            "echo":0,
            "unreachable":0
            }
    corrupted = 0
    invalid = 0
    valid = 0
    ipv6_pkts=0

    print_pkts = lambda x : x <10 or x == 6886

    # Open the pcap file
    if infile.endswith(".gz"):
        open_file = lambda :gzip.open(infile, 'rb')
    else:
        open_file = lambda :open(infile, 'rb')
    with open_file() as f:
        pcap = dpkt.pcap.Reader(f)

        # Open the CSV file for writing
        if outfile.endswith(".gz"):
            write_file = lambda : gzip.open(outfile, 'wt')
        else:
            write_file = lambda : open(outfile, 'w')
        with write_file() as csv_file:
            csv_file.write('Timestamp,SourceIPOuter,DestinationIPOuter,SourceIPInner,DestinationIPInner,PacketSize,SeqNum')

            # Iterate through each packet in the pcap file
            # for timestamp, buf in pcap:
            while True:
                try:
                    (timestamp, buf) = next(pcap)
                except StopIteration:
                    break
                except Exception as e:
                    print(f"Can't parse pcap! File:{infile} : {type(e).__name__}{e}")
                    raise e

                # print(pktid)
                pktid = pktid + 1
                # Parse the Ethernet frame
                try:
                    eth = dpkt.ethernet.Ethernet(buf)
                except Exception as e:
                    print(f"Can't parse ethernet frame: {e}")
                    break

                # Extract the IP and transport layer information
                # if not hasattr(eth, 'data') and not isinstance(eth.data, dpkt.ip.IP):
                #     continue
                if isinstance(eth.data, dpkt.ip.IP):
                    ip_outer = eth.data
                    if isinstance(ip_outer.data, dpkt.udp.UDP) or isinstance(ip_outer.data, dpkt.icmp.ICMP): # TODO: gtp can also be tunneled over tcp?
                        src_port = ip_outer.data.sport
                        dst_port = ip_outer.data.dport
                        # port 2152 => GTP
                        if src_port == 2152 or dst_port == 2152:
                            if len(ip_outer.data) > 50: # TODO: should be checked by validator
                                try:
                                    ip_inner = dpkt.ip.IP(ip_outer.data.data[16:])
                                except Exception as e:
                                    print(f"Can't parse ip frame {pktid}! File:{infile} : {e}")
                                    continue

                                # Convert IP addresses from binary to string format
                                src_ip_outer = socket.inet_ntoa(ip_outer.src)
                                dst_ip_outer = socket.inet_ntoa(ip_outer.dst)

                                if DEBUG:
                                    print(f"UDP:{isinstance(ip_outer.data, dpkt.udp.UDP)}; TCP:{isinstance(ip_outer.data, dpkt.tcp.TCP)}")
                                    print(f"UDP:{isinstance(ip_inner.data, dpkt.udp.UDP)}; TCP:{isinstance(ip_inner.data, dpkt.tcp.TCP)}")
                                    print(f"so:{src_ip_outer}, do:{dst_ip_outer}")
                                    print(f"si:{socket.inet_ntoa(ip_inner.src)}, di:{socket.inet_ntoa(ip_inner.dst)}")
                                    print(f"sp:{ip_inner.data.sport}, dp:{ip_inner.data.dport}")
                                    print(len(ip_inner.data.data))
                                    print(ip_inner.data.data)
                                    print(type(ip_inner))
                                    print(type(ip_inner).__name__)
                                    print("")
                                ret = handle_inner_ipv4(ip_inner, validator, pktid, infile)
                                icmps["echo"]+=ret["icmps"]["echo"]
                                icmps["unreachable"]+=ret["icmps"]["unreachable"]
                                corrupted += ret["corrupted"]
                                invalid += ret["invalid"]
                                if ret["skip"]:
                                    continue

                                if ip_outer.len != len(ip_outer.data)+20:
                                    corrupted +=1
                                    continue

                                # Write the information to the CSV file
                                csv_file.write(f"\n{timestamp},{src_ip_outer},{dst_ip_outer},{ret["ip_src"]},{ret["ip_dst"]},{ip_inner.len},{ret["seqnum"]}")
                                valid += 1
                    else:
                        print("Error2")
                        continue
                else:
                    print("Error1")
    # logging
    status_dict = { "file":f"{os.path.basename(os.path.dirname(infile)) +"/"+ os.path.basename(infile)}", "time":f"{time.time() - t0:.2f}",
                   "total_pkts":pktid, "valid_pkts":valid, "corrupted":corrupted, "invalid":invalid,"icmps":icmps, "ip_v6":f"{ipv6_pkts}" }
    return json.dumps(status_dict)


def parse_pcap_ip(infile, outfile, offset = 0, validator: validator = empty_validator):
    t0 = time.time()
    pktid = 0

    icmps = {
            "echo":0,
            "unreachable":0
            }
    corrupted = 0
    invalid = 0
    valid = 0
    ipv6_pkts=0

    # Open the pcap file
    if infile.endswith(".gz"):
        open_file = lambda :gzip.open(infile, 'rb')
    else:
        open_file = lambda :open(infile, 'rb')
    with open_file() as f:
        pcap = dpkt.pcap.Reader(f)

        # Open the CSV file for writing
        if outfile.endswith(".gz"):
            write_file = lambda : gzip.open(outfile, 'wt')
        else:
            write_file = lambda : open(outfile, 'w')
        with write_file() as csv_file:
            csv_file.write('Timestamp,SourceIPOuter,DestinationIPOuter,SourceIPInner,DestinationIPInner,PacketSize,SeqNum')

            while True:
                try:
                    (timestamp, buf) = next(pcap)
                except StopIteration:
                    break
                except Exception as e:
                    print(f"Can't parse pcap! File:{infile} : {type(e).__name__}{e}")
                    raise e

                pktid = pktid + 1

                try:
                    ip_pkt = dpkt.ip.IP(buf[offset:])
                except Exception as e:
                    try:
                        ip6_pkt = dpkt.ip6.IP6(buf[offset:])
                        ipv6_pkts +=1
                        continue
                    except Exception as ee:
                        print(f"Can't get ip frame({pktid}): {e}; {ee}")
                        break

                # Handle ip packet; udpports, buffer -> ip_src/dst, seqnum, icmps
                ret = handle_inner_ipv4(ip_pkt, validator, pktid, infile)
                icmps["echo"]+=ret["icmps"]["echo"]
                icmps["unreachable"]+=ret["icmps"]["unreachable"]
                corrupted += ret["corrupted"]
                invalid += ret["invalid"]
                if ret["skip"]:
                    continue

                csv_file.write(f"\n{timestamp},NA,NA,{ret["ip_src"]},{ret["ip_dst"]},{ip_pkt.len},{ret["seqnum"]}")
                valid += 1

    # logging
    status_dict = { "file":f"{os.path.basename(os.path.dirname(infile)) +"/"+ os.path.basename(infile)}", "time":f"{time.time() - t0:.2f}",
                   "total_pkts":pktid,"valid_pkts":valid, "corrupted":corrupted,"invalid":invalid,"icmps":icmps, "ip_v6":f"{ipv6_pkts}" }
    return json.dumps(status_dict)


# TODO: icmps are no longer checked for (everything is just invalid)
def handle_inner_ipv4(ip_pkt, traffic_type: validator, pktid = None, infile = None):
    ret = {"ip_src":None, "ip_dst":None, "skip": False, "seqnum":0, "corrupted":0,"invalid":0,"icmps":{"echo":0,"unreachable":0}}
    if ip_pkt.len != len(ip_pkt.data)+20 and ip_pkt.len != len(ip_pkt.data)+46:
        if DEBUG:
            print(f"len check failed: {ip_pkt.len} != {len(ip_pkt.data)}+20")
        ret["skip"] = True
        ret["corrupted"] = 1
        return ret

    if isinstance(ip_pkt.data, dpkt.udp.UDP) or isinstance(ip_pkt.data, dpkt.icmp.ICMP):
        ret["ip_src"] = socket.inet_ntoa(ip_pkt.src)
        ret["ip_dst"] = socket.inet_ntoa(ip_pkt.dst)

        # TODO: move to validator: vvv
        # Check if udp src or dst port is the specified port
        # if isinstance(ip_pkt.data, dpkt.udp.UDP) and set([ip_pkt.data.dport,ip_pkt.data.sport]).difference([udpport]) == set([ip_pkt.data.dport, ip_pkt.data.sport]) :
        #     print("Skip")
        #     ret["skip"] = True
        #     return ret

        if not traffic_type.validate(ip_pkt):
            if DEBUG:
                print(f"Failed validation for pkt# {pktid} in file {infile}")
            ret["skip"] = True
            ret["invalid"] = 1
            return ret



        if isinstance(ip_pkt.data, dpkt.udp.UDP):
            seqnum_dec = int.from_bytes(ip_pkt.data.data[traffic_type.seq_num_first_byte:traffic_type.seq_num_last_byte])
            ret["seqnum"] = seqnum_dec
            # e.g. aaa3419
            # if not seqnum_dec.startswith("aaa") or not seqnum_dec[3:].isdecimal():
            #     ret["skip"] = True
            #     ret["corrupted"] = 1
            #     return ret
            # ret["seqnum"] = seqnum_dec.replace("X", "").replace("a", "")

        elif isinstance(ip_pkt.data, dpkt.icmp.ICMP):
            icmp = ip_pkt.data
            if hasattr(icmp, 'echo'):
                echo = icmp.echo
                ret["seqnum"] = echo.seq
                ret["icmps"]["echo"]=1
            elif hasattr(icmp, 'unreach'):
                ret["icmps"]["unreachable"]=1
                ret["skip"] = True
                return ret
            else:
                icmp.pprint()
                raise ValueError(f"Unknown ICMP packet received!")
        else:
            ret["skip"] = True
    else:
        ret["skip"] = True
    return ret

# Example call: python3 packet-parser.py --infile 20240103132615_gw.pcap --outfile foo.csv --mode ipvlan --content udp
if __name__ == '__main__':

    # Parameter parsing.

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--infile",
        help="Input pcap file. [default is dump.pcap].",
        type=str,
        default="dump.pcap")

    parser.add_argument(
        "--outfile",
        help="Output CSV file. [default is dump.csv].",
        type=str,
        default="dump.csv")

    parser.add_argument(
        "--mode",
        help="Parsing mode. [default is gtp, available: gtp, ip, ipvlan].",
        type=str,
        default="gtp")

    parser.add_argument(
        "--content",
        help="Content to parse. [default is udp, available: udp].",
        type=str,
        default="udp")

    parser.add_argument(
        "--udpport",
        help="UDP port to look for. [default is 6363].",
        type=int,
        default=6363)

    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    mode = args.mode
    content = args.content
    udpport = args.udpport

    # outfile = ".".join(infile.split(".")[:-1]) + ".csv"

    if mode == 'gtp':
        parse_pcap_gtp(infile, outfile, udpport)
    if mode == 'ip':
        parse_pcap_ip(infile, outfile)
    if mode == 'ipvlan':
        parse_pcap_ip(infile, outfile, offset=18)
    if mode == 'ipether':
        parse_pcap_ip(infile, outfile, offset=14)
