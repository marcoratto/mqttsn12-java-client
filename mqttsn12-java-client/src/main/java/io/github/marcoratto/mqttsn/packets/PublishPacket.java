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
import io.github.marcoratto.mqttsn.util.HexUtils;

public class PublishPacket {
  
	private byte length;
    private byte type;
    private byte flags;
    private short topicID;
    private short messageID;
    private byte data[]; 
	
	private boolean extended = false;
	private int lengthExtended;
	
	private int flagDup = 0;
	private int flagQos = 0;
	private int flagRetain = 0;
	private int flagTopicId = 0;
 	
	public PublishPacket() {
		this.length = 0;
		this.type = MqttSnConstants.TYPE_PUBLISH;
		this.flags = 0;
		this.topicID = 0;
		this.messageID = 0;
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
    	.append(",data=")
    	.append(HexUtils.bytesToHex(data));
    	return sb.toString();
    }
    
	public String decodeFlags() {
		StringBuffer sb = new StringBuffer("");
		
		sb.append("DUP=");
		sb.append(this.flagDup);
		
		sb.append(",QOS=");
		sb.append(this.flagQos);
		
		sb.append(",RETAIN=");
		sb.append(this.flagRetain);
		
		sb.append(",Topic ID=");
		sb.append(this.flagTopicId);
		
		return sb.toString();
	}
	
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
    		// Buffer > 65K ?
    		if (this.extended) {
    			this.length = 1;
    			this.lengthExtended = (0x09 + this.data.length);
    			buffer = ByteBuffer.allocate(this.lengthExtended);
    		} else {
    			this.length = (byte) ( 0x07 + this.data.length); 
    			buffer = ByteBuffer.allocate(this.length);
    		}
    		
    		buffer.put(this.length); 
    		// Buffer > 65K ?
    		if (this.extended) {
    			buffer.putShort((short) this.lengthExtended);
    		}    
            buffer.put(this.type);             
            buffer.put(this.flags);        
            buffer.putShort(this.topicID);
            buffer.putShort(this.messageID);
            buffer.put(data);
			
		} catch (Exception e) {
			throw new MqttSnClientException(e);
		}
    	return buffer.array();
    }
    
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		
		this.length = (byte) buffer.get();
		if (this.length == 1) {
			this.extended = true;
			this.lengthExtended = buffer.getShort();
		}
		this.type = (byte) buffer.get();
		this.flags = (byte) buffer.get();
		this.topicID = buffer.getShort();
		this.messageID = buffer.getShort();
		
		// Buffer > 65K ?
		if (this.extended) {
			this.data = new byte[this.lengthExtended-9];
			
			for (int i = 0; i < (this.lengthExtended-9); i++) {
	            byte b = buffer.get();
				this.data[i] = b;
	        }
		} else {
			this.data = new byte[this.length-7];
			
			for (int i = 0; i < (this.length-7); i++) {
	            byte b = buffer.get();
				this.data[i] = b;
	        }			
		}
		this.flagDup = (flags & 0b10000000) >> 7;
		this.flagQos = (flags &  0b1100000) >> 5;
		this.flagRetain = (flags & 0b10000) >> 4;
		this.flagTopicId = (flags &  0b11);	
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

	public short getTopicID() {
		return topicID;
	}
	
	public String getTopicIDAsString() {
		char first = (char) ((topicID >> 8) & 0xFF); // Byte alto
        char second = (char) (topicID & 0xFF);      // Byte basso
        String out = "" + first + second;
        return out;
	}

	public void setTopicID(short value) {
		this.topicID = value;
	}

	public short getMessageID() {
		return messageID;
	}

	public void setMessageID(short value) {
		this.messageID = value;
	}

	public byte[] getData() {
		return data;
	}

	public int getFlagDup() {
		return flagDup;
	}

	public int getFlagQos() {
		return flagQos;
	}

	public int getFlagRetain() {
		return flagRetain;
	}
	
	public boolean isFlagRetain() {
		return (flagRetain == 1);
	}

	public int getFlagTopicId() {
		return flagTopicId;
	}

	public void setData(String value) throws MqttSnClientException {
        if (value != null && value.trim().length() > MqttSnConstants.MAX_PAYLOAD_LENGTH_EXTENDED) {
            throw new MqttSnClientException("Payload '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH_EXTENDED + ")");
        }
        if (value != null && value.trim().length() > MqttSnConstants.MAX_PAYLOAD_LENGTH) {
            // throw new MqttSnClientException("Payload '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH + ")");
        	this.extended = true;
        }
		this.data = value.getBytes();
	}
	
	public void setData(byte[] value) throws MqttSnClientException {
        if (value != null && value.length > MqttSnConstants.MAX_PAYLOAD_LENGTH_EXTENDED) {
            throw new MqttSnClientException("Payload '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH_EXTENDED + ")");
        }
		if (value != null && value.length > MqttSnConstants.MAX_PAYLOAD_LENGTH) {
            // throw new MqttSnClientException("Payload '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH + ")");
			this.extended = true;
        }
        this.data = new byte[value.length];
		this.data = value;
	}
}
