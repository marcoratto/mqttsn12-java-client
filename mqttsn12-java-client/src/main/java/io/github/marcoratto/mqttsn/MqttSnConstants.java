/*
MIT License

Copyright (c) 2025 Marco Ratto

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package io.github.marcoratto.mqttsn;

/*
@ignore
*/
public class MqttSnConstants {

	public final static int DEFAULT_PORT        = 2442;
	public final static byte DEFAULT_TIMEOUT    = 10;
	public final static byte DEFAULT_KEEP_ALIVE = 10;

	public final static int  MAX_PACKET_LENGTH  = 255;
	public final static int  MAX_PAYLOAD_LENGTH = (MAX_PACKET_LENGTH-7);
	public final static int  MAX_TOPIC_LENGTH   = (MAX_PACKET_LENGTH-6);
	public final static byte MAX_CLIENT_ID_LENGTH  = 23;
	public final static byte MAX_WIRELESS_NODE_ID_LENGTH  = (byte) 252;

	/*
	 The byte buffer (the byte array) is the data that is to be sent in the UDP datagram. 
	 The length of the above buffer, 65508 bytes, is the maximum amount of data you can send in a single UDP packet.
	 */
	public final static int  MAX_PACKET_LENGTH_EXTENDED  = 63 * 1024;
	public final static int  MAX_PAYLOAD_LENGTH_EXTENDED = (MAX_PACKET_LENGTH_EXTENDED-7);
	public final static int  MAX_TOPIC_LENGTH_EXTENDED   = (MAX_PACKET_LENGTH_EXTENDED-6);
	
	public final static byte TYPE_ADVERTISE     = 0x00;
	public final static byte TYPE_SEARCHGW      = 0x01;
	public final static byte TYPE_GWINFO        = 0x02;
	public final static byte TYPE_CONNECT       = 0x04;
	public final static byte TYPE_CONNACK       = 0x05;
	public final static byte TYPE_WILLTOPICREQ  = 0x06;
	public final static byte TYPE_WILLTOPIC     = 0x07;
	public final static byte TYPE_WILLMSGREQ    = 0x08;
	public final static byte TYPE_WILLMSG       = 0x09;
	public final static byte TYPE_REGISTER      = 0x0A;
	public final static byte TYPE_REGACK        = 0x0B;
	public final static byte TYPE_PUBLISH       = 0x0C;
	public final static byte TYPE_PUBACK        = 0x0D;
	public final static byte TYPE_PUBCOMP       = 0x0E;
	public final static byte TYPE_PUBREC        = 0x0F;
	public final static byte TYPE_PUBREL        = 0x10;
	public final static byte TYPE_SUBSCRIBE     = 0x12;
	public final static byte TYPE_SUBACK        = 0x13;
	public final static byte TYPE_UNSUBSCRIBE   = 0x14;
	public final static byte TYPE_UNSUBACK      = 0x15;
	public final static byte TYPE_PINGREQ       = 0x16;
	public final static byte TYPE_PINGRESP      = 0x17;
	public final static byte TYPE_DISCONNECT    = 0x18;
	public final static byte TYPE_WILLTOPICUPD  = 0x1A;
	public final static byte TYPE_WILLTOPICRESP = 0x1B;
	public final static byte TYPE_WILLMSGUPD    = 0x1C;
	public final static byte TYPE_WILLMSGRESP   = 0x1D;
	public final static byte TYPE_FRWDENCAP     = (byte) 0xFE;

	public final static byte ACCEPTED               = 0x00;
	public final static byte REJECTED_CONGESTION    = 0x01;
	public final static byte REJECTED_INVALID       = 0x02;
	public final static byte REJECTED_NOT_SUPPORTED = 0x03;

	public final static byte TOPIC_TYPE_NORMAL     = 0x00;
	public final static byte TOPIC_TYPE_PREDEFINED = 0x01;
	public final static byte TOPIC_TYPE_SHORT      = 0x02;

	public final static int FLAG_DUP      = 0x1 << 7;
	public final static byte FLAG_QOS_0    = 0x0 << 5;
	public final static byte FLAG_QOS_1    = 0x1 << 5;
	public final static byte FLAG_QOS_2    = 0x2 << 5;
	public final static byte FLAG_QOS_N1   = 0x3 << 5;
	public final static int FLAG_QOS_MASK = 0x3 << 5;
	public final static int FLAG_RETAIN   = 0x1 << 4;
	public final static int FLAG_WILL     = 0x1 << 3;
	public final static int FLAG_CLEAN    = 0x1 << 2;

	public final static byte PROTOCOL_ID  = 0x01;
	
	public final static int QOS_0    = 0;
	public final static int QOS_1    = 1;
	public final static int QOS_2    = 2;
	public final static int QOS_N1   = -1;
}
