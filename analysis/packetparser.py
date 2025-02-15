import argparse
import csv
import dpkt
import socket
import time
import gzip

def parse_pcap_gtp(infile, outfile, udpport = 6363):
    t0 = time.time()
    print(infile)
    pktid = 0
    # Open the pcap file
    if infile.endswith(".gz"):
        open_file = lambda :gzip.open(infile, 'rb')
    else:
        open_file = lambda :open(infile, 'rb')
    with open_file() as f:
        pcap = dpkt.pcap.Reader(f)

        # Open the CSV file for writing
        with open(outfile, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)

            # Write the header row
            writer.writerow(['Timestamp', 'SourceIPOuter', 'DestinationIPOuter', 'SourceIPInner', 'DestinationIPInner',
                             'PacketSize', 'SeqNum'])

            # Iterate through each packet in the pcap file
            # for timestamp, buf in pcap:
            while True:
                try:
                    (timestamp, buf) = next(pcap)
                except Exception as e:
                    print(e)
                    break

                # print(pktid)
                pktid = pktid + 1
                # Parse the Ethernet frame
                try:
                    eth = dpkt.ethernet.Ethernet(buf)
                except Exception as e:
                    print(e)
                    break

                # Extract the IP and transport layer information
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
                                    print(e)
                                    continue
                                src_ip_inner = socket.inet_ntoa(ip_inner.src)
                                dst_ip_inner = socket.inet_ntoa(ip_inner.dst)

                                # Convert IP addresses from binary to string format
                                src_ip_outer = socket.inet_ntoa(ip_outer.src)
                                dst_ip_outer = socket.inet_ntoa(ip_outer.dst)

                                if isinstance(ip_inner.data, dpkt.udp.UDP)or isinstance(ip_inner.data, dpkt.icmp.ICMP):
                                    if isinstance(ip_inner.data, dpkt.udp.UDP) and ip_inner.data.dport not in [udpport]:
                                        continue
                                    if isinstance(ip_inner.data, dpkt.udp.UDP):
                                        seqnum = ip_inner.data.data.decode().replace("X", "").replace("a", "")
                                    elif isinstance(ip_inner.data, dpkt.icmp.ICMP):
                                        icmp = ip_inner.data
                                        echo = icmp.echo
                                        # print(icmp.data)
                                        # print(echo)
                                        # print('ICMP: type:%d code:%d checksum:%d data: %s\n' % (icmp.type, icmp.code, icmp.sum, repr(icmp.data)))
                                        seqnum = echo.seq


                                        # Write the information to the CSV file
                                    writer.writerow([
                                        timestamp,
                                        src_ip_outer,
                                        dst_ip_outer,
                                        src_ip_inner,
                                        dst_ip_inner,
                                        ip_outer.len + 14,
                                        seqnum
                                    ])

                    else:
                        continue

    print("Took %.2fsec" % (time.time() - t0))


def parse_pcap_ip(infile, outfile, offset = 0, udpport = 6363):
    t0 = time.time()
    print(infile)
    pktid = 0
    # Open the pcap file
    if infile.endswith(".gz"):
        open_file = lambda :gzip.open(infile, 'rb')
    else:
        open_file = lambda :open(infile, 'rb')
    with open_file() as f:
        pcap = dpkt.pcap.Reader(f)

        # Open the CSV file for writing
        with open(outfile, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)

            # Write the header row
            writer.writerow(['Timestamp', 'SourceIPOuter', 'DestinationIPOuter', 'SourceIPInner', 'DestinationIPInner',
                             'PacketSize', 'SeqNum'])

            # Iterate through each packet in the pcap file
            # for timestamp, buf in pcap:
            while True:
                try:
                    (timestamp, buf) = next(pcap)
                except Exception as e:
                    print(e)
                    break

                pktid = pktid + 1
                # print(pktid)

                # if len(buf) <= 64:
                #     continue

                try:
                    ip_pkt = dpkt.ip.IP(buf[offset:])
                except Exception as e:
                    print(f"Can't get ip frame: {e}")
                    continue

                if isinstance(ip_pkt.data, dpkt.udp.UDP) or isinstance(ip_pkt.data, dpkt.icmp.ICMP):
                    ip_src = socket.inet_ntoa(ip_pkt.src)
                    ip_dst = socket.inet_ntoa(ip_pkt.dst)

                    if isinstance(ip_pkt.data, dpkt.udp.UDP) and ip_pkt.data.dport not in [udpport]:
                        continue
                    if isinstance(ip_pkt.data, dpkt.udp.UDP):
                        seqnum = ip_pkt.data.data.decode().replace("X", "").replace("a", "")
                    elif isinstance(ip_pkt.data, dpkt.icmp.ICMP):
                        icmp = ip_pkt.data
                        echo = icmp.echo
                        # print(icmp.data)
                        # print(echo)
                        # print('ICMP: type:%d code:%d checksum:%d data: %s\n' % (icmp.type, icmp.code, icmp.sum, repr(icmp.data)))
                        seqnum = echo.seq

                    writer.writerow([
                        timestamp,
                        'NA',
                        'NA',
                        ip_src,
                        ip_dst,
                        ip_pkt.len,
                        seqnum
                    ])

    print("Took %.2fsec" % (time.time() - t0))

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
