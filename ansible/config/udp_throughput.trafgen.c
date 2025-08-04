/* This file need to be run with --cpp for c-preprocessor call.

Command example:
 trafgen --cpp --dev enp5s0 --conf udp_sample.trafgen.c --cpu 1 --verbose --gap 1000us
*/

{
  /* --- Ethernet Header --- */
  /* MAC DST */
  0xdc, 0xa6, 0x32, 0x20, 0x3f, 0xd9,
  /* MAC SRC */
  0xe4, 0x5f, 0x01, 0x05, 0xee, 0x17,
  const16(0x0800),

  /* --- IPv4 Header --- */
  0b01000101, 0,  /* IPv4 Version, IHL, TOS */
  const16(46),    /* IPv4 Total Len (UDP len + IP hdr 20 bytes)*/
  const16(2),     /* IPv4 Ident */
  0b01000000, 0,  /* IPv4 Flags, Frag Off */
  64,             /* IPv4 TTL */
  17,             /* Proto UDP */
  csumip(14, 33), /* IPv4 Checksum (IP header from, to) */

  /* --- UDP Header --- */
  /* IP SRC */
  10, 0, 0, 1
  /* IP DST */
  10, 0, 0, 2

  const16(9),    /* UDP Source Port e.g. drnd(2)*/
  const16(1234), /* UDP Dest Port */
  const16(20),   /* UDP length (UDP hdr 8 bytes + payload size */

  /* UDP checksum can be dyn calc via csumudp(offset IP, offset TCP)
   * which is csumudp(14, 34), but for UDP its allowed to be zero
   */
  const16(0),

  /*** Payload ***/
  0x53,0x59,0x43,0x31,
  //seqinc(0,1,4294967295)
  //seqinc(0,1,4294967295)
  //seqinc(0,1,4294967295)
  rnd(4),
  //dinc(1,65535,1),
  //dinc(1,65535,1),
  //dinc(1,65535,1),
  0x00,0x00,0x00,dinc(1,65535,1),
  //seqinc(0,65535,1)
  //seqinc(0,65535,1)
  //seqinc(0,65535,1)
  //dinc(0,4294967295,1)
  //dinc(0,4294967295,1)
  //dinc(0,4294967295,1)
  //dinc(0,4294967295,1)
  //drnd(8),
}
