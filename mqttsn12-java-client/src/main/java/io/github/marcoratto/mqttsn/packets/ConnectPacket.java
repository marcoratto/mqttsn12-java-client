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

public class ConnectPacket {

	private byte length;
	private byte type;
	private byte flags;
	private byte protocolID;
	private short duration;
    private byte clientID[];
    
	private int flagWill;
	private int flagCleanSession;
	private int flagTopicId;   
    
    public ConnectPacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_CONNECT;
    	this.flags = 0;
    	this.protocolID = 0;
    	this.duration = 0;
    }
    
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("length=")
    	.append(this.length)
    	.append(",type=")
    	.append(this.type)
    	.append(",flags=")
    	.append(this.flags)
    	.append(",protocolID=")
    	.append(this.protocolID)
    	.append(",duration=")
    	.append(this.duration)
    	.append(",clientID=")
    	.append(new String(this.clientID));
    	return sb.toString();
    }
    
	public String decodeFlags() {
		StringBuffer sb = new StringBuffer("");
		
		sb.append("WILL=");
		sb.append(this.flagWill);
		
		sb.append(",QOS=");
		sb.append(this.flagCleanSession);
			
		sb.append(",Topic ID=");
		sb.append(this.flagTopicId);
		
		return sb.toString();
	}
    
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		
		this.length = (byte) buffer.get();
		this.type = (byte) buffer.get();
		this.flags = (byte) buffer.get();
		this.protocolID = (byte) buffer.get();
		this.duration = buffer.getShort();
		this.clientID = new byte[this.length - 6];
		
		for (int i = 0; i < (this.length-6); i++) {
            byte b = buffer.get();
			this.clientID[i] = b;
        }		
		
		this.flagWill = (flags & 0b1000) >> 3;
		this.flagCleanSession = (flags &  0b100) >> 2;
		this.flagTopicId = (flags &  0b11);
	}
    
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
    		this.length = (byte) (6 + this.clientID.length);
    		buffer = ByteBuffer.allocate(this.length);

            // Inserire i dati nel buffer
            buffer.put(length);          // Inserisce l'int (4 byte)
            buffer.put(type);               // Inserisce il byte unsigned
            buffer.put(flags);        	// Inserisce la stringa grezza (byte array)
            buffer.put(protocolID);
            buffer.putShort(duration);
            buffer.put(clientID);
			
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

	public byte getProtocolID() {
		return protocolID;
	}

	public void setProtocolID(byte protocol_id) {
		this.protocolID = protocol_id;
	}

	public short getDuration() {
		return duration;
	}

	public void setDuration(short duration) {
		this.duration = duration;
	}

	public byte[] getClientID() {
		return clientID;
	}

	public int getFlagWill() {
		return flagWill;
	}
	
	public boolean isFlagWill() {
		return (flagWill == 1);
	}

	public int getFlagCleanSession() {
		return flagCleanSession;
	}
	
	public boolean isFlagCleanSession() {
		return (flagCleanSession == 1);
	}

	public int getFlagTopicId() {
		return flagTopicId;
	}

	public void setClientID(String value) throws MqttSnClientException {
        if (value != null && value.trim().length() > MqttSnConstants.MAX_CLIENT_ID_LENGTH) {
            throw new MqttSnClientException("Client ID '" + value + "' is too long (max " + MqttSnConstants.MAX_CLIENT_ID_LENGTH + ")");
        }
        this.clientID = new byte[value.trim().length()];
		this.clientID = value.trim().getBytes();
	}
    
} 

