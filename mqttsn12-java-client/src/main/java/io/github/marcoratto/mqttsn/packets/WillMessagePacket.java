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

public class WillMessagePacket {

	private byte length;
	private byte type;
    private byte[] message;
    
    public WillMessagePacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_WILLMSG;
    	this.message = null;
    }
    
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("length=")
    	.append(this.length)
    	.append(",type=")
    	.append(this.type)
    	.append(",data=")
    	.append(HexUtils.bytesToHex(message));
    	return sb.toString();
    }
    
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		
		this.length = (byte) buffer.get();
		this.type = (byte) buffer.get();

		this.message = new byte[this.length-2];
		
		for (int i = 0; i < (this.length-2); i++) {
            byte b = buffer.get();
			this.message[i] = b;
        }			

	}
    
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
    		this.length = (byte) (2 + this.message.length);
    		buffer = ByteBuffer.allocate(length);
            buffer.put(this.length);          
            buffer.put(this.type); 
            buffer.put(this.message);
		} catch (Exception e) {
			throw new MqttSnClientException(e);
		}
    	return buffer.array();
    }

	public byte getType() {
		return type;
	}
	
	public byte getLength() {
		return this.length;
	}

	public String getMessage() {
		return new String(this.message, 0, this.message.length);
	}

	public void setMessage(String value) throws MqttSnClientException {
        if (value != null && value.trim().length() > MqttSnConstants.MAX_PAYLOAD_LENGTH) {
            throw new MqttSnClientException("Will Message '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH + ")");
        }
        this.message = new byte[value.trim().length()];
		this.message = value.trim().getBytes();
	}
	
	public void setMessage(byte[] value) throws MqttSnClientException {
	   this.message = new byte[value.length];
       this.message = value;
	}
		
}
