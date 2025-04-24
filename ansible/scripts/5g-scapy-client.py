#!/usr/bin/env python3
import socket
from scapy.all import *
import numpy as np
from scapy.layers.inet import *


def main() -> int:
    import argparse
    argparser = argparse.ArgumentParser()
    argparser.add_argument("protocol", help="Protocol used for data transfer")
    argparser.add_argument("iat_dist", default="det", help="IAT distribution det or exp")
    argparser.add_argument("arrival_rate", default=-1,
                           help="Arrival Rate for det = wanted IAT, for exp = lambda = 1/arrival_rate")
    argparser.add_argument("payload_size", default="small", help="Small (just counter) | Big (1400 + counter)")
    argparser.add_argument("packet_amount", default="small", help="Number of packets to transmit")
    argparser.add_argument("dst_ip", default="172.30.2.2", help="Destination IP address")
    argparser.add_argument("dst_port", default=6363, help="Destination port")
    argparser.add_argument("burst_size", default=1,
                           help="Number of packets per burst, each burst spaced with sum(bust_size IATs)")
    args = argparser.parse_args()

    if args.protocol == "tcp":
        generate_tcp(args.dst_ip, int(args.dst_port), args.iat_dist, float(args.arrival_rate), args.payload_size,
                     int(args.packet_amount), int(args.burst_size))
    elif args.protocol == "udp":
        generate_udp(args.dst_ip, int(args.dst_port), args.iat_dist, float(args.arrival_rate), args.payload_size,
                     int(args.packet_amount), int(args.burst_size))
    elif args.protocol == "icmp":
        generate_icmp(args.dst_ip, args.iat_dist, float(args.arrival_rate), args.payload_size, int(args.packet_amount),
                      int(args.burst_size))


def generate_tcp(dst_ip, dst_port, iat_dist, arrival_rate, payload_size, packet_amount, burst_size):
    sck = socket.socket(socket.AF_INET, socket.TCP_NODELAY)
    sck.connect((dst_ip, dst_port))
    ssck = StreamSocket(sck)
    send_packets(ssck, iat_dist, arrival_rate, payload_size, packet_amount, "tcp", burst_size)


def generate_udp(dst_ip, dst_port, iat_dist, arrival_rate, payload_size, packet_amount, burst_size):
    sck = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sck.connect((dst_ip, dst_port))
    ssck = StreamSocket(sck)
    send_packets(ssck, iat_dist, arrival_rate, payload_size, packet_amount, "udp", burst_size)


def generate_icmp(dst_ip, iat_dist, arrival_rate, payload_size, packet_amount, burst_size):
    sck = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    sck.connect((dst_ip, 1234))
    ssck = StreamSocket(sck)
    send_packets(ssck, iat_dist, arrival_rate, payload_size, packet_amount, "icmp", burst_size)


def apply_burst_size(pkt_sched, burst_size):
    iats = np.diff(pkt_sched)
    burst_iats = np.add.reduceat(iats, (np.arange(np.ceil(len(iats) / burst_size)) * burst_size).astype('int64'))
    sched_out = np.zeros(len(iats))
    sched_out[0::burst_size] = burst_iats
    sched_out = np.cumsum(sched_out)
    return sched_out


def send_packets(socket, iat_dist, arrival_rate, payload_size, packet_amount, packet_type, burst_size):
    if payload_size == "small":
        payload = "aaaa"
        index = 1
    else:
        payload = "X" * 1400
        index = 1
    t0 = time.perf_counter()
    if iat_dist == "det":
        # arbitrary initial offset of few sec
        pktschedule = t0 + 3 + (np.array(range(0, packet_amount)) * arrival_rate)
    elif iat_dist == "exp":
        pktschedule = t0 + 3 + np.cumsum(np.random.exponential(arrival_rate, packet_amount))
    if burst_size > 1:
        pktschedule = t0 + 3 + apply_burst_size((np.array(range(0, packet_amount + 1)) * arrival_rate), burst_size)

    # tlast = t0
    assert(packet_amount <= 0xffffffff)
    for i in range(0, packet_amount):
        payload_n = payload.encode() + index.to_bytes(4, "big")
        if packet_type != "icmp":
            pkt_n = Raw(payload_n)
        else:
            pkt_n = ICMP(id=1234, seq=0) / payload_n
        high_precision_sleep(pktschedule[i] - time.perf_counter())
        # tpre = time.perf_counter()
        socket.send(pkt_n)
        # tpost = time.perf_counter()
        # print("took %.2f mus to send()" % ((tpost - tpre) / 1e-6))
        # print("sent pkt %d at %.2f us since t0, iat %.2f us" % (index, (tpost - t0) / 1e-6, (tpost - tlast) / 1e-6))
        # tlast = tpost
        index += 1


def high_precision_sleep(duration):
    start_time = time.perf_counter()
    while True:
        elapsed_time = time.perf_counter() - start_time
        remaining_time = duration - elapsed_time
        if remaining_time <= 0:
            break
        if remaining_time > 0.02:  # Sleep for 5ms if remaining time is greater
            time.sleep(max(remaining_time / 2, 0.0001))  # Sleep for the remaining time or minimum sleep interval
        else:
            pass


if __name__ == "__main__":
    sys.exit(main())
