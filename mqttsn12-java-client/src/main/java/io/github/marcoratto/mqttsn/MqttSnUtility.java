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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSnUtility {

	private static final Logger logger = LoggerFactory.getLogger(MqttSnUtility.class);

	public final static byte getQosFlag(int qos) throws MqttSnClientException {
		byte out = 0;
		switch (qos) {
		case MqttSnConstants.QOS_N1:
			out = MqttSnConstants.FLAG_QOS_N1;
			break;

		case MqttSnConstants.QOS_0:
			out = MqttSnConstants.FLAG_QOS_0;
			break;

		case MqttSnConstants.QOS_1:
			out = MqttSnConstants.FLAG_QOS_1;
			break;

		case MqttSnConstants.QOS_2:
			throw new MqttSnClientException("QOS=" + qos + " not supported");

		default:
			throw new MqttSnClientException("QOS=" + qos + " not valid");
		}
		logger.debug("QOS:" + out);
		return out;
	}
	
	public final static int decodeQos(byte flags) throws MqttSnClientException {
		
		if (((flags & MqttSnConstants.FLAG_QOS_0) != 0)) {
			return MqttSnConstants.QOS_0;
		}
		if (((flags & MqttSnConstants.FLAG_QOS_1) != 0)) {
			return MqttSnConstants.QOS_1;
		}
		if (((flags & MqttSnConstants.FLAG_QOS_2) != 0)) {
			throw new MqttSnClientException("QOS=2 not supported");
		}
		if (((flags & MqttSnConstants.FLAG_QOS_N1) != 0)) {
			return MqttSnConstants.QOS_0;
		}
		return 0;
	}

	public final static String decodeReturnCode(byte return_code) {
		switch (return_code) {
		case MqttSnConstants.ACCEPTED:
			return "Accepted (" + return_code + ")";
		case MqttSnConstants.REJECTED_CONGESTION:
			return "Rejected: congestion (" + return_code + ")";
		case MqttSnConstants.REJECTED_INVALID:
			return "Rejected: invalid topic ID (" + return_code + ")";
		case MqttSnConstants.REJECTED_NOT_SUPPORTED:
			return "Rejected: not supported (" + return_code + ")";
		default:
			return "" + return_code;
		}
	}

	public final static String decodeType(byte type) {
		switch (type) {
		case MqttSnConstants.TYPE_ADVERTISE:
			return "ADVERTISE";
		case MqttSnConstants.TYPE_SEARCHGW:
			return "SEARCHGW";
		case MqttSnConstants.TYPE_GWINFO:
			return "GWINFO";
		case MqttSnConstants.TYPE_CONNECT:
			return "CONNECT";
		case MqttSnConstants.TYPE_CONNACK:
			return "CONNACK";
		case MqttSnConstants.TYPE_WILLTOPICREQ:
			return "WILLTOPICREQ";
		case MqttSnConstants.TYPE_WILLTOPIC:
			return "WILLTOPIC";
		case MqttSnConstants.TYPE_WILLMSGREQ:
			return "WILLMSGREQ";
		case MqttSnConstants.TYPE_WILLMSG:
			return "WILLMSG";
		case MqttSnConstants.TYPE_REGISTER:
			return "REGISTER";
		case MqttSnConstants.TYPE_REGACK:
			return "REGACK";
		case MqttSnConstants.TYPE_PUBLISH:
			return "PUBLISH";
		case MqttSnConstants.TYPE_PUBACK:
			return "PUBACK";
		case MqttSnConstants.TYPE_PUBCOMP:
			return "PUBCOMP";
		case MqttSnConstants.TYPE_PUBREC:
			return "PUBREC";
		case MqttSnConstants.TYPE_PUBREL:
			return "PUBREL";
		case MqttSnConstants.TYPE_SUBSCRIBE:
			return "SUBSCRIBE";
		case MqttSnConstants.TYPE_SUBACK:
			return "SUBACK";
		case MqttSnConstants.TYPE_UNSUBSCRIBE:
			return "UNSUBSCRIBE";
		case MqttSnConstants.TYPE_UNSUBACK:
			return "UNSUBACK";
		case MqttSnConstants.TYPE_PINGREQ:
			return "PINGREQ";
		case MqttSnConstants.TYPE_PINGRESP:
			return "PINGRESP";
		case MqttSnConstants.TYPE_DISCONNECT:
			return "DISCONNECT";
		case MqttSnConstants.TYPE_WILLTOPICUPD:
			return "WILLTOPICUPD";
		case MqttSnConstants.TYPE_WILLTOPICRESP:
			return "WILLTOPICRESP";
		case MqttSnConstants.TYPE_WILLMSGUPD:
			return "WILLMSGUPD";
		case MqttSnConstants.TYPE_WILLMSGRESP:
			return "WILLMSGRESP";
		case MqttSnConstants.TYPE_FRWDENCAP:
			return "FRWDENCAP";
		default:
			return "UNKNOWN";
		}
	}

}
