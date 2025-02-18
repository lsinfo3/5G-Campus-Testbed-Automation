import argparse
import dpkt
import socket
import time
import gzip
import os
import json
import binascii

def parse_pcap_gtp(infile, outfile, udpport = 6363):
    t0 = time.time()
    pktid = 0

    icmps = {
            "echo":0,
            "unreachable":0
            }
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
            csv_file.write('Timestamp, SourceIPOuter, DestinationIPOuter, SourceIPInner, DestinationIPInner, PacketSize, SeqNum')

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
                    if isinstance(ip_outer.data, dpkt.udp.UDP) or isinstance(ip_outer.data, dpkt.icmp.ICMP):
                        src_port = ip_outer.data.sport
                        dst_port = ip_outer.data.dport
                        # port 2152 => GTP
                        if src_port == 2152 or dst_port == 2152:
                            if len(ip_outer.data) > 50:
                                try:
                                    ip_inner = dpkt.ip.IP(ip_outer.data.data[16:])
                                except Exception as e:
                                    print(f"Can't parse ip frame: {e}")
                                    continue

                                # Convert IP addresses from binary to string format
                                src_ip_outer = socket.inet_ntoa(ip_outer.src)
                                dst_ip_outer = socket.inet_ntoa(ip_outer.dst)

                                ret = handle_inner_ipv4(ip_inner, udpport)
                                icmps["echo"]+=ret["icmps"]["echo"]
                                icmps["unreachable"]+=ret["icmps"]["unreachable"]
                                if ret["skip"]:
                                    continue

                                # Write the information to the CSV file
                                csv_file.write(f"\n{timestamp},{src_ip_outer},{dst_ip_outer},{ret["ip_src"]},{ret["ip_dst"]},{ip_outer.len+14},{ret["seqnum"]}")
                    else:
                        continue
    # logging
    status_dict = { "file":f"{os.path.basename(os.path.dirname(infile)) +"/"+ os.path.basename(infile)}", "time":f"{time.time() - t0:.2f}",
                   "pkts":f"{pktid}", "icmps":icmps, "ip_v6":f"{ipv6_pkts}" }
    return json.dumps(status_dict)


def parse_pcap_ip(infile, outfile, offset = 0, udpport = 6363):
    t0 = time.time()
    pktid = 0

    icmps = {
            "echo":0,
            "unreachable":0
            }
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
            csv_file.write('Timestamp, SourceIPOuter, DestinationIPOuter, SourceIPInner, DestinationIPInner, PacketSize, SeqNum')

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
                ret = handle_inner_ipv4(ip_pkt, udpport)
                icmps["echo"]+=ret["icmps"]["echo"]
                icmps["unreachable"]+=ret["icmps"]["unreachable"]
                if ret["skip"]:
                    continue

                csv_file.write(f"\n{timestamp},NA,NA,{ret["ip_src"]},{ret["ip_dst"]},{ip_pkt.len},{ret["seqnum"]}")

    # logging
    status_dict = { "file":f"{os.path.basename(os.path.dirname(infile)) +"/"+ os.path.basename(infile)}", "time":f"{time.time() - t0:.2f}",
                   "pkts":f"{pktid}", "icmps":icmps, "ip_v6":f"{ipv6_pkts}" }
    return json.dumps(status_dict)

def handle_inner_ipv4(ip_pkt, udpport):
    ret = {"ip_src":None, "ip_dst":None, "skip": False, "seqnum":0, "icmps":{"echo":0,"unreachable":0}}
    if isinstance(ip_pkt.data, dpkt.udp.UDP) or isinstance(ip_pkt.data, dpkt.icmp.ICMP):
        ret["ip_src"] = socket.inet_ntoa(ip_pkt.src)
        ret["ip_dst"] = socket.inet_ntoa(ip_pkt.dst)

        if isinstance(ip_pkt.data, dpkt.udp.UDP) and ip_pkt.data.dport not in [udpport]:
            ret["skip"] = True
            return ret
        if isinstance(ip_pkt.data, dpkt.udp.UDP):
            ret["seqnum"] = ip_pkt.data.data.decode().replace("X", "").replace("a", "")
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
        parse_pcap_ip(infile, outfile, udpport=udpport)
    if mode == 'ipvlan':
        parse_pcap_ip(infile, outfile, offset=18, udpport=udpport)
    if mode == 'ipether':
        parse_pcap_ip(infile, outfile, offset=14, udpport=udpport)
