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

import io.github.marcoratto.mqttsn.MqttSnConstants;


public class GatewayInfoPacket {

    private byte length;
    private byte type;
    private byte gwID;
    private byte gwAddress[]; 
	
	public GatewayInfoPacket() {
		this.length = 0;
		this.type = MqttSnConstants.TYPE_GWINFO;
	}
    
	public void decode(byte[] value) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		
		this.length = (byte) buffer.get();
		this.type = (byte) buffer.get();
		this.gwID = (byte) buffer.get();
		
		this.gwAddress = new byte[this.length-3];
		
		for (int i = 0; i < (this.length-3); i++) {
            byte b = buffer.get();
			this.gwAddress[i] = b;
        }
	}

	public byte getLength() {
		return length;
	}
	
	public byte getType() {
		return type;
	}

	public byte getGatewayID() {
		return gwID;
	}

	public String getGatewayAddress() {
		return new String(this.gwAddress);
	}

}
