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

public class UnsubscribePacket {

    private byte length;
    private byte type;
    private byte flags;
    private short messageID;
    private byte[] topicName;
    private short  topicID;
    
    public UnsubscribePacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_UNSUBSCRIBE;
    	this.flags = 0;
    	this.messageID = 0;
    	this.topicName = null;
    	this.topicID = 0;
    }
    
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
    		if (this.topicName != null) {
    			this.length = (byte) (0x05 + this.topicName.length);
    		} else {
    			this.length = (byte) 7;
    		}
    		
    		buffer = ByteBuffer.allocate(this.length);

            // Inserire i dati nel buffer
            buffer.put(length);         
            buffer.put(type);            
            buffer.put(flags);        	
            buffer.putShort(messageID);
            
            if (this.topicName != null) {
            	buffer.put(topicName);	
            } else {
            	buffer.putShort(topicID);	
            }
            
		} catch (Exception e) {
			throw new MqttSnClientException(e);
		}
    	return buffer.array();
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

	public void setFlags(byte flags) {
		this.flags = flags;
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
	}
	
	public void setTopicName(byte[] value) throws MqttSnClientException {
        if (value != null && value.length > MqttSnConstants.MAX_TOPIC_LENGTH) {
            throw new MqttSnClientException("TopicName '" + value + "' is too long (max " + MqttSnConstants.MAX_TOPIC_LENGTH + ")");
        }
        this.topicName = new byte[value.length];
		this.topicName = value;
	}

	public short getTopicID() {
		return topicID;
	}

	public void setTopicID(short topicID) {
		this.topicID = topicID;
	}
    
    
}
