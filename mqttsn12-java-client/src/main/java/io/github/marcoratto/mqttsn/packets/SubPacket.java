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

public class SubPacket {

    private byte length;
    private byte type;
    private byte flags;
    private short messageID;
    private byte[] topicName;
    private short  topicID;
    
	private int flagTopicId = 0;
    
    public SubPacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_SUBSCRIBE;
    	this.flags = 0;
    	this.messageID = 0;
    	this.topicName = null;
    	this.topicID = 0;
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
    	.append(",topicName=")
    	.append(new String(topicName));
    	return sb.toString();
    }
    
	public String decodeFlags() {
		StringBuffer sb = new StringBuffer("");		
		return sb.toString();
	}
	
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		
		this.length = (byte) buffer.get();
		this.type = (byte) buffer.get();
		this.flags = (byte) buffer.get();
		this.messageID = buffer.getShort();
		
		if (this.length == 5) {
			this.topicID = buffer.getShort();
		} else {
			this.topicName = new byte[this.length-5];
			
			for (int i = 0; i < (this.length-5); i++) {
	            byte b = buffer.get();
				this.topicName[i] = b;
	        }	
		}
		
		this.flagTopicId = (flags &  0b11);
	}
    
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
    		if (this.topicName == null) {
    			this.length = (byte) 7;
    		} else {
    			this.length = (byte) (0x05 + this.topicName.length);	
    		}
    		buffer = ByteBuffer.allocate(this.length);

            // Inserire i dati nel buffer
            buffer.put(length);         
            buffer.put(type);            
            buffer.put(flags);        	
            buffer.putShort(messageID);
            
            if (this.topicName == null) {
            	buffer.putShort(topicID);          		
            } else {
            	buffer.put(topicName);	
            }
            
		} catch (Exception e) {
			throw new MqttSnClientException(e);
		}
    	return buffer.array();
    }
    
	public int getFlagTopicId() {
		return flagTopicId;
	}

	public byte getLength() {
		return length;
	}

	public byte getType() {
		return type;
	}

	public byte getFlags() {
		return flags;
	}

	public void setFlags(byte value) {
		this.flags = value;
	}

	public short getMessageID() {
		return messageID;
	}

	public void setMessageID(short messageID) {
		this.messageID = messageID;
	}

	public String getTopicName() {
		return new String(topicName);
	}

	public void setTopicName(String value) throws MqttSnClientException {
        if (value != null && value.trim().length() > MqttSnConstants.MAX_TOPIC_LENGTH) {
            throw new MqttSnClientException("TopicName '" + value + "' is too long (max " + MqttSnConstants.MAX_TOPIC_LENGTH + ")");
        }
		this.topicName = value.getBytes();
		this.topicID = 0;
	}
	
	public void setTopicName(byte[] value) throws MqttSnClientException {
        if (value != null && value.length > MqttSnConstants.MAX_TOPIC_LENGTH) {
            throw new MqttSnClientException("TopicName '" + value + "' is too long (max " + MqttSnConstants.MAX_TOPIC_LENGTH + ")");
        }
        this.topicName = new byte[value.length];
		this.topicName = value;
		this.topicID = 0;
	}

	public short getTopicID() {
		return topicID;
	}

	public void setTopicID(short value) {
		this.topicName = null;
		this.topicID = value;
	}
    
    
}
