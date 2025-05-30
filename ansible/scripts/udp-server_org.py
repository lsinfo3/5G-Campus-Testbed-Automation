#!/usr/bin/env python3

"""
Python UDP Listener
"""
import socket
import argparse



def bind_socket(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, port))

    while True:
       data, address = sock.recvfrom(60784)
       print(address)
       sock.sendto(data, address)



if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-i", "--ip", help="IP address to bind to.", required=True)
    argparser.add_argument("-p", "--port", help="Port to bind to.", required=True)
    args = argparser.parse_args()
    bind_socket(ip=args.ip, port=int(args.port))


