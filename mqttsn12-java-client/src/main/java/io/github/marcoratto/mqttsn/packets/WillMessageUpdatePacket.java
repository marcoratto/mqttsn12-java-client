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

public class WillMessageUpdatePacket {

    private byte length;
    private byte type;
    private byte[] message;
    
    public WillMessageUpdatePacket() {
    	this.length = 0;
    	this.type = MqttSnConstants.TYPE_WILLMSGUPD;
    	this.message = null;
    }
    
    public byte[] encode() throws MqttSnClientException {
    	ByteBuffer buffer = null;

    	try {
    		this.length = (byte) (2 + message.length);
    		buffer = ByteBuffer.allocate(this.length);

            // Inserire i dati nel buffer
            buffer.put(length);          
            buffer.put(type);  
            buffer.put(message);
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

	public String getMessage() {
		return new String(message);
	}

	public void setMessage(String value) throws MqttSnClientException {
        if (value != null && value.trim().length() > MqttSnConstants.MAX_PAYLOAD_LENGTH) {
            throw new MqttSnClientException("Will Message '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH + ")");
        }
        this.message = new byte[value.trim().length()];
		this.message = value.trim().getBytes();
	}

	public void setMessage(byte[] value) throws MqttSnClientException {
        if (value != null && value.length > MqttSnConstants.MAX_PAYLOAD_LENGTH) {
            throw new MqttSnClientException("Will Message '" + value + "' is too long (max " + MqttSnConstants.MAX_PAYLOAD_LENGTH + ")");
        }
        this.message = new byte[value.length];
		this.message = value;
	}
}
