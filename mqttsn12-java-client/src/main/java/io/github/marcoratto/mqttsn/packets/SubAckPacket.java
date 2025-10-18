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
package io.github.marcoratto.mqttsn.packets;

import java.nio.ByteBuffer;

import io.github.marcoratto.mqttsn.MqttSnClientException;
import io.github.marcoratto.mqttsn.MqttSnConstants;

public class SubAckPacket {

	private byte length;
	private byte type;
	private byte flags;
	private short topicID;
	private short messageID;
    private byte returnCode;
    
    public SubAckPacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_SUBACK;
    	this.flags = 0;
    	this.topicID = 0;
    	this.messageID = 0;
    	this.returnCode = 0;
    }
    
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("length=")
    	.append(this.length)
    	.append(",type=")
    	.append(this.type)
    	.append(",flags=")
    	.append(this.flags)
    	.append(",topicID=")
    	.append(this.topicID)
    	.append(",messageID=")
    	.append(this.messageID)
    	.append(",returnCode=")
    	.append(returnCode);
    	return sb.toString();
    }
    
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		this.length = (byte) buffer.get();
		this.type = (byte) buffer.get();
		this.flags = (byte) buffer.get();
		this.topicID = (short) buffer.getShort();
		this.messageID = (short) buffer.getShort();
		this.returnCode = (byte) buffer.get();
	}

	   public byte[] encode() throws MqttSnClientException {
	    	ByteBuffer buffer = null;

	    	try {
	    		this.length = (byte) 8;

	    		buffer = ByteBuffer.allocate(this.length);

	            // Inserire i dati nel buffer
	            buffer.put(length);         
	            buffer.put(type);            
	            buffer.put(flags);  
	            buffer.putShort(topicID);
	            buffer.putShort(messageID);
	            buffer.put(returnCode);
			} catch (Exception e) {
				throw new MqttSnClientException(e);
			}
	    	return buffer.array();
	    }

	public byte getFlags() {
		return flags;
	}

	public void setFlags(byte flags) {
		this.flags = flags;
	}

	public short getTopicID() {
		return topicID;
	}

	public void setTopicID(short topicID) {
		this.topicID = topicID;
	}

	public short getMessageID() {
		return messageID;
	}

	public void setMessageID(short messageID) {
		this.messageID = messageID;
	}

	public byte getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(byte returnCode) {
		this.returnCode = returnCode;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}
	   
    
}
