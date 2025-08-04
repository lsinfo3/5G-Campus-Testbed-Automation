#include <stdint.h>

// Define your sequence number as a macro that expands at runtime
// __uint32 seq_num __seq

{
eth(dst=ff:ff:ff:ff:ff:ff, src=00:11:22:33:44:55)
ip(saddr=192.168.1.1, daddr=192.168.1.2, ttl=64, proto=udp)
udp(sport=12345, dport=54321)

// Insert 4-byte sequence number (big endian)
// uint32(seq_num)
uint32(__seq)

// Fill the rest of the packet with dummy data to reach desired size
pad(1400 - 4)  // 4 bytes already used by the sequence number
}
