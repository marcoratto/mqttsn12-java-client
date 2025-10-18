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

public class DisconnectReqPacket {

    private byte length;
    private byte  type;
    private short duration;
    
    public DisconnectReqPacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_DISCONNECT;
    	this.duration = 0;
    }
    
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("length=")
    	.append(this.length)
    	.append(",type=")
    	.append(this.type)
    	.append(",duration=")
    	.append(this.duration);
    	return sb.toString();
    }
    
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
            if (this.duration == 0) {
            	this.length = (byte) 0x02;              
            } else {
            	this.length = (byte) 0x04;
            }
    		buffer = ByteBuffer.allocate(this.length);

            buffer.put(length);       
            buffer.put(type);               
            if (this.length == 4) {
            	 buffer.putShort(duration);
            } 
		} catch (Exception e) {
			throw new MqttSnClientException(e);
		}
    	return buffer.array();
    }
    
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		this.length = (byte) buffer.get();
		this.type = (byte) buffer.get();
		if (this.length == 4) {
			this.duration = (short) buffer.getShort();
		}
	}

	public byte getLength() {
		return length;
	}

	public byte getType() {
		return type;
	}

	public short getDuration() {
		return duration;
	}

	public void setDuration(short duration) {
		this.duration = duration;
	}
    
    
}
