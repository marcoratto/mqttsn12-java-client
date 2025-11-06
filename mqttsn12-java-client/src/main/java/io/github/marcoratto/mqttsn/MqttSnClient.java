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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.marcoratto.mqttsn.packets.ConnackPacket;
import io.github.marcoratto.mqttsn.packets.ConnectPacket;
import io.github.marcoratto.mqttsn.packets.DisconnectReqPacket;
import io.github.marcoratto.mqttsn.packets.DisconnectResPacket;
import io.github.marcoratto.mqttsn.packets.GatewayInfoPacket;
import io.github.marcoratto.mqttsn.packets.PingReqPacket;
import io.github.marcoratto.mqttsn.packets.PubAckPacket;
import io.github.marcoratto.mqttsn.packets.PubRecPacket;
import io.github.marcoratto.mqttsn.packets.PubRelPacket;
import io.github.marcoratto.mqttsn.packets.PubCompPacket;
import io.github.marcoratto.mqttsn.packets.PublishPacket;
import io.github.marcoratto.mqttsn.packets.RegackPacket;
import io.github.marcoratto.mqttsn.packets.RegisterPacket;
import io.github.marcoratto.mqttsn.packets.SearchGatewayPacket;
import io.github.marcoratto.mqttsn.packets.SubAckPacket;
import io.github.marcoratto.mqttsn.packets.SubPacket;
import io.github.marcoratto.mqttsn.packets.UnsubackPacket;
import io.github.marcoratto.mqttsn.packets.UnsubscribePacket;
import io.github.marcoratto.mqttsn.packets.WillMessagePacket;
import io.github.marcoratto.mqttsn.packets.WillMessageReqPacket;
import io.github.marcoratto.mqttsn.packets.WillMessageResPacket;
import io.github.marcoratto.mqttsn.packets.WillMessageUpdatePacket;
import io.github.marcoratto.mqttsn.packets.WillTopicPacket;
import io.github.marcoratto.mqttsn.packets.WillTopicReqPacket;
import io.github.marcoratto.mqttsn.packets.WillTopicResPacket;
import io.github.marcoratto.mqttsn.packets.WillTopicUpdateReqPacket;
import io.github.marcoratto.mqttsn.util.HexUtils;

public class MqttSnClient {
	
	public final static String VERSION = "1.1.0";
	
	private final static Logger logger = LoggerFactory.getLogger(MqttSnClient.class);
	
	public static void main(String[] args) {
		System.out.println(VERSION);
	}

	public MqttSnClient() {
		this.port = MqttSnConstants.DEFAULT_PORT;
		this.timeout = MqttSnConstants.DEFAULT_TIMEOUT;
		this.keepAlive = MqttSnConstants.DEFAULT_KEEP_ALIVE;
		this.clientID = "mqtt-sn-java-" + (new Random().nextInt() & 0xffff);
		this.nextMessageID = 1;		
		this.willMessage = null;
		this.willTopic = null;
		this.willQos = 0;
		this.willRetain = false;
		this.connected = false;
		this.reuseAddress = true;
	}

    public void open(String host, int port) throws MqttSnClientException {
        try {
        	this.open(InetAddress.getByName(host), port);
		} catch (UnknownHostException e) {
			throw new MqttSnClientException(e);
		} 
        
    }
    
    public void open(InetAddress host, int port) throws MqttSnClientException {
		this.port = port;
		this.address = host;
		try {
        	this.datagramSocket = new DatagramSocket();
        	this.datagramSocket.setSoTimeout((int) this.getTimeoutMillis());
        	this.datagramSocket.setReuseAddress(this.reuseAddress);
			logger.debug("Socket opened.");
			this.connected = true;
		} catch (SocketException e) {
			throw new MqttSnClientException(e);
		}   
    }
    
    public void close() throws MqttSnClientException {
    	if (datagramSocket != null) {
    		logger.debug("Socket closed.");
   			datagramSocket.close();	
    	}
    	this.connected = false;
    }
    
    public boolean isConnected() {
		return connected;
	}
    
    public void sendSubscribe(String topic_filter, int qos, MqttSnListener aMqttSnCallback) throws MqttSnClientException {
    	int topic_len = topic_filter.length();
        SubPacket subPacket = new SubPacket();
     
        byte flags = 0x00;
        flags += MqttSnUtility.getQosFlag(qos);
        
        if (topic_len == 2) {
            flags += MqttSnConstants.TOPIC_TYPE_SHORT;
            byte[] topic_name = topic_filter.getBytes();
            short topic_id = (short) ((topic_name[0] << 8) + topic_name[1]);
            subPacket.setTopicID(topic_id);
        } else {
            flags += MqttSnConstants.TOPIC_TYPE_NORMAL;
            subPacket.setTopicName(topic_filter);
        }
        subPacket.setFlags((byte) flags);
        subPacket.setMessageID((short) this.nextMessageID++);
        
        this.sendPacket(subPacket.encode());
        
        short topic_id = this.receiveSuback();
        
       	if ((topic_id > 0) && (topic_len > 2)) {
			this.registerTopic(topic_id, topic_filter);     
			this.addMqttSnCallback(topic_filter, aMqttSnCallback);
	    } else if ((topic_id == 0) && (topic_len == 2)) {
        	// topic_id = this.sendRegister(topic);
            byte[] topic_name = topic_filter.getBytes();
            topic_id = (short) ((topic_name[0] << 8) + topic_name[1]); 
            this.addMqttSnCallback(topic_id, aMqttSnCallback);
	    } else {
	    	this.addMqttSnCallback(topic_filter, aMqttSnCallback);
	    }
       	
    }
     
    public void sendSubscribe(short topic_id, int qos, MqttSnListener aMqttSnCallback) throws MqttSnClientException {
    	SubPacket subscribePacket = new SubPacket();

        byte flags = 0x00;
        flags += MqttSnUtility.getQosFlag(qos);
        flags += MqttSnConstants.TOPIC_TYPE_PREDEFINED;
        subscribePacket.setFlags((byte) flags);
        
        subscribePacket.setMessageID((short) this.nextMessageID++);
        subscribePacket.setTopicID(topic_id);

        this.sendPacket(subscribePacket.encode());
        
        this.receiveSuback();
        
        this.addMqttSnCallback(topic_id, aMqttSnCallback);
    }
    
    public void sendUnSubscribe(String topic_name) throws MqttSnClientException {
        int topic_name_len = topic_name.length();
        UnsubscribePacket unsubscribePacket = new UnsubscribePacket();

        byte flags = 0;
        
        if (topic_name_len == 2) {
            flags += MqttSnConstants.TOPIC_TYPE_SHORT;
        } else {
            flags += MqttSnConstants.TOPIC_TYPE_NORMAL;
        }
        unsubscribePacket.setFlags((byte) flags);
        unsubscribePacket.setMessageID((short) this.nextMessageID++);
        unsubscribePacket.setTopicName(topic_name);
        this.sendPacket(unsubscribePacket.encode());
        
        this.receiveUnsuback();
        
        Short topic_id = this.searchTopicId(topic_name);
        if (topic_id != null) {
        	this.unregisterTopic(topic_id);
        }   
    }
    
    public void sendUnSubscribe(short topic_id) throws MqttSnClientException {
    	UnsubscribePacket unsubscribePacket = new UnsubscribePacket();
        byte flags = 0x00;
        flags += MqttSnConstants.TOPIC_TYPE_PREDEFINED;
        unsubscribePacket.setFlags((byte) flags);
        
        unsubscribePacket.setMessageID((short) this.nextMessageID++);
        unsubscribePacket.setTopicID(topic_id);

        this.sendPacket(unsubscribePacket.encode());
        
        this.receiveUnsuback();
        
        this.unregisterTopic(topic_id);
    }
    
    public void sendWillMessageUpdate(String aWillMessage) throws MqttSnClientException {
    	this.willMessage = aWillMessage.getBytes();
    	
    	byte[] response;
        WillMessageUpdatePacket willMessageUpdatePacket = new WillMessageUpdatePacket();
        willMessageUpdatePacket.setMessage(this.willMessage);
        this.sendPacket(willMessageUpdatePacket.encode());
                
        response = this.receivePacketSync();
        
        if (response == null) {
            throw new MqttSnClientException("Failed to connect to MQTT-SN gateway.");
        }
        WillMessageResPacket willMessageRespPacket = new WillMessageResPacket();
        willMessageRespPacket.decode(response);
        
        if (willMessageRespPacket.getType() != MqttSnConstants.TYPE_WILLMSGRESP) {
        	throw new MqttSnClientException("Was expecting WILLMSGRESP packet but received: " + MqttSnUtility.decodeType(willMessageRespPacket.getType()));
        }

        // Check Connack return code
        logger.debug("WILLMSGRESP return code:" + MqttSnUtility.decodeReturnCode(willMessageRespPacket.getReturnCode()));

        if (willMessageRespPacket.getReturnCode() > 0) {
        	throw new MqttSnClientException("WILLMSGRESP error: " + MqttSnUtility.decodeReturnCode(willMessageRespPacket.getReturnCode()));  
        }
    }
    
    public void sendWillTopicUpdate() throws MqttSnClientException {
        WillTopicUpdateReqPacket willTopicUpdateReqPacket = new WillTopicUpdateReqPacket();
        
        byte flags = 0;

        if (this.willRetain) {
        	flags += MqttSnConstants.FLAG_RETAIN;
        }
            
        flags |= (byte) MqttSnUtility.getQosFlag(willQos);
        
        willTopicUpdateReqPacket.setFlags(flags);
        willTopicUpdateReqPacket.setTopicName(this.willTopic);
        
        this.sendPacket(willTopicUpdateReqPacket.encode());
                
        byte[] response = this.receivePacketSync();
        
        if (response == null) {
            throw new MqttSnClientException("Failed to connect to MQTT-SN gateway.");
        }
        WillTopicResPacket willTopicResPacket = new WillTopicResPacket();
        willTopicResPacket.decode(response);
        
        if (willTopicResPacket.getType() != MqttSnConstants.TYPE_WILLTOPICRESP) {
        	throw new MqttSnClientException("Was expecting WILLTOPICRESP packet but received: " + MqttSnUtility.decodeType(willTopicResPacket.getType()));
        }

        // Check Connack return code
        logger.debug("WILLTOPICRESP return code:" + willTopicResPacket.getReturnCode());

        if (willTopicResPacket.getReturnCode() > 0) {
        	throw new MqttSnClientException("WILLTOPICRESP error: " + MqttSnUtility.decodeReturnCode(willTopicResPacket.getReturnCode()));  
        }
    }
    
    public void sendConnect() throws MqttSnClientException {
    	byte[] response;
    	
    	ConnectPacket connectPacket = new ConnectPacket();
    	
    	byte flags = 0;
    	if (this.cleanSession) {
    		flags += MqttSnConstants.FLAG_CLEAN;
    	}
        if ((this.willTopic != null) && (this.willMessage != null)) {
        	flags += MqttSnConstants.FLAG_WILL;	
        }
    	
	   	 // Create the CONNECT packet
	   	connectPacket.setFlags((byte) flags);
	   	connectPacket.setProtocolID(MqttSnConstants.PROTOCOL_ID);
	   	connectPacket.setDuration(this.keepAlive);
	   	connectPacket.setClientID(this.clientID);

        this.sendPacket(connectPacket.encode());
        
        if ((this.willTopic != null) && (this.willMessage != null)) {
        	logger.debug("LWT enabled.");
            response = this.receivePacketSync();
            
            if (response == null) {
                throw new MqttSnClientException("Failed to connect to MQTT-SN gateway.");
            }
            
            WillTopicReqPacket willTopicReqPacket = new WillTopicReqPacket();
            willTopicReqPacket.decode(response);
            
            if (willTopicReqPacket.getType() != MqttSnConstants.TYPE_WILLTOPICREQ) {
            	throw new MqttSnClientException("Was expecting WILLTOPICREQ packet but received: " + MqttSnUtility.decodeType(willTopicReqPacket.getType()));
            }

            WillTopicPacket willTopicPacket = new WillTopicPacket();
            
            flags = 0;

            if (this.willRetain) {
            	flags += MqttSnConstants.FLAG_RETAIN;
            }
                
            flags |= (byte) MqttSnUtility.getQosFlag(willQos);

        	willTopicPacket.setTopicName(willTopic);
        	willTopicPacket.setFlags((byte) flags);

        	
            this.sendPacket(willTopicPacket.encode());
            response = this.receivePacketSync();
            
            if (response == null) {
                throw new MqttSnClientException("Failed to connect to MQTT-SN gateway.");
            }
            
            WillMessageReqPacket willMessageReqPacket = new WillMessageReqPacket();
            willMessageReqPacket.decode(response);
            
            if (willMessageReqPacket.getType() != MqttSnConstants.TYPE_WILLMSGREQ) {
            	throw new MqttSnClientException("Was expecting WILLMSGREQ packet but received: " + MqttSnUtility.decodeType(willMessageReqPacket.getType()));
            }
            
            WillMessagePacket willMessagePacket = new WillMessagePacket();
            willMessagePacket.setMessage(willMessage);

            this.sendPacket(willMessagePacket.encode());
            
        }
        
        response = this.receivePacketSync();
        
        if (response == null) {
            throw new MqttSnClientException("Failed to connect to MQTT-SN gateway.");
        }
        ConnackPacket connackPacket = new ConnackPacket();
        connackPacket.decode(response);
        
        if (connackPacket.getType() != MqttSnConstants.TYPE_CONNACK) {
        	throw new MqttSnClientException("Was expecting CONNACK packet but received: " + MqttSnUtility.decodeType(connackPacket.getType()));
        }

        // Check Connack return code
        logger.debug("CONNACK return code:" + MqttSnUtility.decodeReturnCode(connackPacket.getReturnCode()));

        if (connackPacket.getReturnCode() > 0) {
        	throw new MqttSnClientException("CONNECT error: " + MqttSnUtility.decodeReturnCode(connackPacket.getReturnCode()));  
        }
    }
    
    public void sendDisconnect() throws MqttSnClientException
    {
    	this.sendDisconnect((short) 0);
    }
    
    public void sendDisconnect(short duration) throws MqttSnClientException
    {
        DisconnectReqPacket disconnectReqPacket = new DisconnectReqPacket();
       	disconnectReqPacket.setDuration(duration);

        this.sendPacket(disconnectReqPacket.encode());
        
        byte[] response = this.waitFor(MqttSnConstants.TYPE_DISCONNECT);
        // byte[] response = this.receivePacketSync();
        if (response == null) {
            throw new MqttSnClientException("Failed to disconnect from MQTT-SN gateway.");
        }
        
        DisconnectResPacket disconnectResPacket = new DisconnectResPacket();
        disconnectResPacket.decode(response);
        
        if (disconnectResPacket.getType() != MqttSnConstants.TYPE_DISCONNECT) {
        	throw new MqttSnClientException("Was expecting DISCONNECT packet but received: " + MqttSnUtility.decodeType(disconnectResPacket.getType()));
        }

        // Check Disconnect return duration
        if (disconnectResPacket.getLength() == 4) {
            logger.warn("DISCONNECT warning. Gateway returned duration in disconnect packet.");
        }
    }
    
    public void sendSearchGateway(byte radius) throws MqttSnClientException
    {
    	SearchGatewayPacket searchGatewayPacket = new SearchGatewayPacket();
    	searchGatewayPacket.setRadius(radius);
    	
    	this.sendPacket(searchGatewayPacket.encode());
    	
        // byte[] response = this.receivePacketSync();
        byte[] response = this.waitFor(MqttSnConstants.TYPE_GWINFO);
        if (response == null) {
            throw new MqttSnClientException("Failed to disconnect from MQTT-SN gateway.");
        }
    	
    	GatewayInfoPacket gatewayInfoPacket = new GatewayInfoPacket();
    	gatewayInfoPacket.decode(response);
    	
        if (gatewayInfoPacket.getType() != MqttSnConstants.TYPE_GWINFO) {
        	throw new MqttSnClientException("Was expecting GWINFO packet but received: " + MqttSnUtility.decodeType(gatewayInfoPacket.getType()));
        }
        logger.info("Gateway ID: " + gatewayInfoPacket.getGatewayID());
        logger.info("Gateway Address: " + gatewayInfoPacket.getGatewayAddress());
    }
      
    public short sendPublish(String topic_name, byte[] data, int qos, boolean retain) throws MqttSnClientException {
    	
    	byte topic_type = MqttSnConstants.TOPIC_TYPE_NORMAL;

    	short topic_id = this.sendRegister(topic_name);

        this.sendPublish(topic_id, topic_type, data, qos, retain);
        
        return topic_id;
    }
      
    public short sendRegister(String topic) throws MqttSnClientException {
    
        int topic_name_len = topic.length();
        RegisterPacket packet = new RegisterPacket();

        if (topic_name_len > MqttSnConstants.MAX_TOPIC_LENGTH) {
            throw new MqttSnClientException("Topic name is too long");
        }

        packet.setTopicID((byte) 0);
        packet.setMessageID((byte) this.nextMessageID++);
        packet.setTopicName(topic);
        this.sendPacket(packet.encode());
        
        short topic_id = this.receiveRegack();
        
        return topic_id;
    }
    
    public void sendPublishShort(short topic_id, byte[] data, int qos, boolean retain) throws MqttSnClientException {
    	this.sendPublish(topic_id, MqttSnConstants.TOPIC_TYPE_SHORT, data, qos, retain);
    }
    
    public void sendPublishPreDefined(short topic_id, byte[] data, int qos, boolean retain) throws MqttSnClientException {
    	this.sendPublish(topic_id, MqttSnConstants.TOPIC_TYPE_PREDEFINED, data, qos, retain);
    }
    
    public void sendPublish(byte[] topic_name, byte[] data, int qos, boolean retain) throws MqttSnClientException {
    	if (topic_name.length != 2) {
    		throw new MqttSnClientException("Paramater 'topic_name' must to be 2 bytes!");
    	}
    	short topic_id = (short) ((topic_name[0] << 8) + topic_name[1]);
    	this.sendPublish(topic_id, MqttSnConstants.TOPIC_TYPE_SHORT, data, qos, retain);
    }
    
    public synchronized void sendPublish(short topic_id, byte topic_type, byte[] data, int qos, boolean retain) throws MqttSnClientException {
    	
    	byte flags = 0;
    	
        if (retain) {
        	flags += MqttSnConstants.FLAG_RETAIN;
        }
            
        flags += (byte) MqttSnUtility.getQosFlag(qos);
        flags += (topic_type & 0x3);
        
    	PublishPacket publishPacket = new PublishPacket();
        publishPacket.setFlags((byte) flags);        
        publishPacket.setTopicID(topic_id);
        if (qos > 0) {
        	publishPacket.setMessageID((short) this.nextMessageID++);
        } else {
        	publishPacket.setMessageID((short) 0x0000);
        }
        publishPacket.setData(data);

        this.sendPacket(publishPacket.encode());

        if (qos == MqttSnConstants.QOS_1) {
            this.receivePuback();
        } else if (qos == MqttSnConstants.QOS_2) {
			short msgId = this.receivePubRec();
			
			this.sendPubRel(msgId);
			
			this.receivePubComp();
		}
    }
    
    public void polling() throws MqttSnClientException {
    	byte[] buffer = this.waitFor(MqttSnConstants.TYPE_PUBLISH);
        if (buffer != null) {
        	PublishPacket publishPacket = new PublishPacket();
        	publishPacket.decode(buffer);
        	
            if (publishPacket.getType() != MqttSnConstants.TYPE_PUBLISH) {
            	throw new MqttSnClientException("Was expecting PUBLISH packet but received: " + MqttSnUtility.decodeType(publishPacket.getType()));
            }
        	
            byte packet_qos = (byte) (publishPacket.getFlags() & MqttSnConstants.FLAG_QOS_MASK);
            if (packet_qos == MqttSnConstants.FLAG_QOS_1) {
            	this.sendPuback(publishPacket.getTopicID(), publishPacket.getMessageID(), MqttSnConstants.ACCEPTED);
            } else if (packet_qos == MqttSnConstants.FLAG_QOS_2) {
			    this.sendPubRec(publishPacket.getMessageID());				    
			    this.receivePubRel();
			    this.sendPubComp(publishPacket.getMessageID());
			}
            
        	short topic_id = publishPacket.getTopicID();
        	String topic_name = this.topicMap.get(publishPacket.getTopicID());
        	logger.debug("topic ID is " + topic_id);
        	logger.debug("topic name is " + topic_name);
        	
        	MqttSnListener mqttSnCallback = null;
        	if (topic_name != null) {
        		mqttSnCallback = this.listOfMqttSnCallback.get(topic_name);
        	} else {
        		mqttSnCallback = this.listOfMqttSnCallback.get(Short.toString(topic_id));
        	}
        	if (mqttSnCallback == null) {
        		logger.warn("Listener for topic name not found. Search by Topic ID");
                for (Map.Entry<String, MqttSnListener> entry : this.listOfMqttSnCallback.entrySet()) {
                    String filter = entry.getKey();
                    MqttSnListener callback = entry.getValue();
                    if (isMatched(topic_name, filter)) {
                    	logger.debug("Found listener for topicID=" + topic_id + ",topic name=" + topic_name + ", topic filter=" + filter);
                    	callback.messageArrived(topic_id, topic_name, publishPacket.getData());
                    }
                }
            } else {
            	mqttSnCallback.messageArrived(topic_id, topic_name, publishPacket.getData());
            }
        } 
    }

	private short receivePuback() throws MqttSnClientException {
        byte[] buffer = this.waitFor(MqttSnConstants.TYPE_PUBACK);
        // byte[] buffer = this.receivePacketSync();
        short received_message_id, received_topic_id;

        if (buffer == null) {
            throw new MqttSnClientException ("Failed to subscribe to topic.");
        }
        PubAckPacket pubackPacket = new PubAckPacket();
        pubackPacket.decode(buffer);
        
        if (pubackPacket.getType() != MqttSnConstants.TYPE_PUBACK) {
        	throw new MqttSnClientException("Was expecting PUBACK packet but received: " + MqttSnUtility.decodeType(pubackPacket.getType()));
        }
        
        // Check Suback return code
        logger.debug("PUBACK return code: " + pubackPacket.getReturnCode());

        if (pubackPacket.getReturnCode() > 0) {
        	throw new MqttSnClientException("PUBLISH error: " + MqttSnUtility.decodeReturnCode(pubackPacket.getReturnCode()));       
        }

        // Check that the Message ID matches
        received_message_id = pubackPacket.getMessageID();
        if (received_message_id != this.nextMessageID-1) {
            logger.warn("Message id in PUBACK does not equal message id sent");
            logger.debug("Expecting: " + (this.nextMessageID-1));
            logger.debug("Actual: " + received_message_id);
        }

        // Return the topic ID returned by the gateway
        received_topic_id = pubackPacket.getTopicID();
        logger.debug("PUBACK topic id: " + received_topic_id);
        return received_topic_id;
    }
    
    private short receivePubRec() throws MqttSnClientException {
        byte[] buffer = this.waitFor(MqttSnConstants.TYPE_PUBREC);
        short received_message_id, received_topic_id;

        if (buffer == null) {
            throw new MqttSnClientException ("Failed to subscribe to topic.");
        }
        PubRecPacket pubrecPacket = new PubRecPacket();
        pubrecPacket.decode(buffer);
        
        if (pubrecPacket.getType() != MqttSnConstants.TYPE_PUBREC) {
        	throw new MqttSnClientException("Was expecting PUBREC packet but received: " + MqttSnUtility.decodeType(pubrecPacket.getType()));
        }
        
        // Check that the Message ID matches
        received_message_id = pubrecPacket.getMessageID();
        if (received_message_id != this.nextMessageID-1) {
            logger.warn("Message id in PUBREC does not equal message id sent! Expecting: " + (this.nextMessageID-1) + ", Actual: " + received_message_id);
        }
        return received_message_id;
    }
    
    private short receivePubComp() throws MqttSnClientException {
        byte[] buffer = this.waitFor(MqttSnConstants.TYPE_PUBCOMP);
        short received_message_id, received_topic_id;

        if (buffer == null) {
            throw new MqttSnClientException ("Failed to subscribe to topic.");
        }
        PubCompPacket pubcompPacket = new PubCompPacket();
        pubcompPacket.decode(buffer);
        
        if (pubcompPacket.getType() != MqttSnConstants.TYPE_PUBCOMP) {
        	throw new MqttSnClientException("Was expecting PUBCOMP packet but received: " + MqttSnUtility.decodeType(pubcompPacket.getType()));
        }
        
        // Check that the Message ID matches
        received_message_id = pubcompPacket.getMessageID();
        if (received_message_id != this.nextMessageID-1) {
            logger.warn("Message id in PUBCOMP does not equal message id sent! Expecting: " + (this.nextMessageID-1) + ", Actual: " + received_message_id);
        }
        return received_message_id;
    }
    
    private short receiveSuback() throws MqttSnClientException {
    	short received_message_id, received_topic_id;
    	
    	byte[] buffer = this.waitFor(MqttSnConstants.TYPE_SUBACK);
    	        
        if (buffer == null) {
            throw new MqttSnClientException ("Failed to subscribe to topic.");
        }
        SubAckPacket packet = new SubAckPacket();
        packet.decode(buffer);
        
        if (packet.getType() != MqttSnConstants.TYPE_SUBACK) {
        	throw new MqttSnClientException("Was expecting SUBACK packet but received: " + MqttSnUtility.decodeType(packet.getType()));
        }
        
        // Check Suback return code
        logger.debug("SUBACK return code: " + packet.getReturnCode());

        if (packet.getReturnCode() > 0) {
        	throw new MqttSnClientException("SUBSCRIBE error: " + MqttSnUtility.decodeReturnCode(packet.getReturnCode()));
        }

        // Check that the Message ID matches
        received_message_id = packet.getMessageID();
        if (received_message_id != this.nextMessageID-1) {
            logger.warn("Message id in SUBACK does not equal message id sent");
            logger.debug("Expecting: " + (this.nextMessageID-1));
            logger.debug("Actual: " + received_message_id);
        }

        // Return the topic ID returned by the gateway
        received_topic_id = packet.getTopicID();
        logger.debug("SUBACK topic id: " + received_topic_id);
        return received_topic_id;
    }
    
    private void receiveUnsuback() throws MqttSnClientException {
    	short received_message_id;
    	byte[] buffer = this.waitFor(MqttSnConstants.TYPE_UNSUBACK);
        
        if (buffer == null) {
            throw new MqttSnClientException ("Failed to subscribe to topic.");
        }
        UnsubackPacket unsubackPacket = new UnsubackPacket();
        unsubackPacket.decode(buffer);
        
        if (unsubackPacket.getType() != MqttSnConstants.TYPE_UNSUBACK) {
        	throw new MqttSnClientException("Was expecting UNSUBACK packet but received: " + MqttSnUtility.decodeType(unsubackPacket.getType()));
        }

        // Check that the Message ID matches
        received_message_id = unsubackPacket.getMessageID();
        if (received_message_id != this.nextMessageID-1) {
            logger.warn("Message id in UNSUBACK does not equal message id sent");
            logger.debug("Expecting: " + (this.nextMessageID-1));
            logger.debug("Actual: " + received_message_id);
        }
    }
    
    private void sendPuback(short topicID, short messageID, byte retCode) throws MqttSnClientException {
        PubAckPacket packet = new PubAckPacket();
        packet.setTopicID(topicID);
        packet.setMessageID(messageID);
        packet.setReturnCode(retCode);
        logger.debug("Sending PUBACK packet...");    
        this.sendPacket(packet.encode());
    }
    
    private void sendPubRel(short messageID) throws MqttSnClientException {
        PubRelPacket packet = new PubRelPacket();
        packet.setMessageID(messageID);
        logger.debug("Sending PUBREL packet...");    
        this.sendPacket(packet.encode());
    }

    private void sendPubRec(short messageID) throws MqttSnClientException {
        PubRecPacket packet = new PubRecPacket();
        packet.setMessageID(messageID);
        logger.debug("Sending PUBREC packet...");    
        this.sendPacket(packet.encode());
    }
    
    private void sendPubComp(short messageID) throws MqttSnClientException {
        PubCompPacket packet = new PubCompPacket();
        packet.setMessageID(messageID);
        logger.debug("Sending PUBCOMP packet...");    
        this.sendPacket(packet.encode());
    }
    
    private void receivePubRel() throws MqttSnClientException {
    	byte[] buffer = this.waitFor(MqttSnConstants.TYPE_PUBREL);
        if (buffer == null) {
            throw new MqttSnClientException("Failed to recceive UDP message.");
        }
        PubRelPacket packet = new PubRelPacket();
        packet.decode(buffer);
        if (packet.getType() != MqttSnConstants.TYPE_PUBREL) {
        	throw new MqttSnClientException("Was expecting PUBREL packet but received: " + MqttSnUtility.decodeType(packet.getType()));
        }
        logger.debug("Received PUBREL packet");
	}
        
    private short receiveRegack() throws MqttSnClientException {
    	byte[] buffer = this.waitFor(MqttSnConstants.TYPE_REGACK);
        if (buffer == null) {
            throw new MqttSnClientException("Failed to connect to register topic.");
        }
        short received_message_id, received_topic_id;
        RegackPacket packet = new RegackPacket();
        packet.decode(buffer);
        
        if (packet.getType() != MqttSnConstants.TYPE_REGACK) {
        	throw new MqttSnClientException("Was expecting REGACK packet but received: " + MqttSnUtility.decodeType(packet.getType()));
        }        

        int retCode = packet.getReturnCode();
        // Check Regack return code
        logger.debug("REGACK return code: " + retCode);
        
        
        if (retCode > 0) {
        	throw new MqttSnClientException("REGISTER failed: " + MqttSnUtility.decodeReturnCode(packet.getReturnCode()));         
        }

        // Check that the Message ID matches
        received_message_id = packet.getMessageID();
        if (received_message_id != this.nextMessageID-1) {
            logger.warn("Message id in Regack does not equal message id sent");
        }

        // Return the topic ID returned by the gateway
        received_topic_id = packet.getTopicID();
        logger.debug("REGACK topic id:" + received_topic_id);

        return received_topic_id;
    }
    
    private void processRegister(byte[] packet) throws MqttSnClientException {
    	RegisterPacket registerPacket = new RegisterPacket();
    	registerPacket.decode(packet);
    	
        if (registerPacket.getType() != MqttSnConstants.TYPE_REGISTER) {
        	throw new MqttSnClientException("Was expecting REGISTER packet but received: " + MqttSnUtility.decodeType(registerPacket.getType()));
        }
    	
        short message_id = registerPacket.getMessageID();
        short topic_id = registerPacket.getTopicID();
        String topic_name = registerPacket.getTopicName();

        // Add it to the topic map
        this.registerTopic(topic_id, topic_name);
        
        // Respond to gateway with REGACK
        this.sendRegack(topic_id, message_id);
    }
    
    void sendRegack(short topic_id, short message_id) throws MqttSnClientException {
        RegackPacket regackPacket = new RegackPacket();
        regackPacket.setMessageID(message_id);
        regackPacket.setReturnCode((byte) 0);
        regackPacket.setTopicID(topic_id);
        this.sendPacket(regackPacket.encode());
    }
    
    private byte[] waitFor(byte type) throws MqttSnClientException {
    	logger.debug("Expecting '" + MqttSnUtility.decodeType(type) + "' message");
    	long startedWaiting = System.currentTimeMillis();

    	boolean running = true;
    	
        while(running) {
            long now = System.currentTimeMillis();

            // Time to send a ping?
            if (this.keepAlive > 0 && (now - lastTransmit) >= this.getKeepAliveMillis()) {
                this.sendPingReq();
            }

                // byte[] buf = this.receivePacket();
            	byte[] buf = this.receivePacketASync();
                if (buf != null) {
                	
                	byte msgType = buf[1];
                	logger.debug("Received '" + MqttSnUtility.decodeType(msgType) + "' message");
                    switch(msgType) {

                        case MqttSnConstants.TYPE_REGISTER:
                        	this.processRegister(buf);
                            break;
                            
                        case MqttSnConstants.TYPE_ADVERTISE:
                            running = false;
                            break;

                        case MqttSnConstants.TYPE_DISCONNECT:
                            if (type != MqttSnConstants.TYPE_DISCONNECT) {
                                throw new MqttSnClientException("Received DISCONNECT from gateway.");
                                // logger.warn("Was expecting '" + MqttSnUtility.decodeType(type) + "' packet but received: " + MqttSnUtility.decodeType(msgType));    
                            }
                            break;

                        default:
                            if (msgType != type) {
                                logger.warn("Was expecting '" + MqttSnUtility.decodeType(type) + "' packet but received: " + MqttSnUtility.decodeType(msgType));    
                            }
                            // TODO: da ritestare
                            // break;
                    }

                    // Did we find what we were looking for?
                    if (msgType == type) {
                        return buf;
                    }
                }


            // Check for receive timeout
            if (this.keepAlive > 0 && (now - this.lastReceive) >= (this.getKeepAliveMillis() * 1.5)) {
            	logger.warn("Keep alive error: timed out while waiting for a '" + MqttSnUtility.decodeType(type) + "' from gateway."); 
            	break;
            }

            // Check if we have timed out waiting for the packet we are looking for
            if ((now - startedWaiting) >= this.getTimeoutMillis()) {
                logger.warn("Timed out while waiting for a '" + MqttSnUtility.decodeType(type) +"' from gateway.");
                break;
            }
        }

        return null;
    }
    
    private void registerTopic(Short topic_id, String topic_name) throws MqttSnClientException {
        
        // Check topic ID is valid
        if (topic_id == 0x0000 || topic_id == 0xFFFF) {
            throw new MqttSnClientException("Attempted to register invalid topic id: " + topic_id);
        }

        // Check topic name is valid
        if (topic_name == null || topic_name.length() <= 0 || topic_name.length() > MqttSnConstants.MAX_TOPIC_LENGTH) {
        	throw new MqttSnClientException("Attempted to register invalid topic name.");
        }

        logger.debug("Registering topic " + topic_id + "=" + topic_name);

       this.topicMap.put(topic_id, topic_name);
    }
    
    private void unregisterTopic(Short topic_id) throws MqttSnClientException {
        
        // Check topic ID is valid
        if (topic_id == 0x0000 || topic_id == 0xFFFF) {
            throw new MqttSnClientException("Attempted to register invalid topic id: " + topic_id);
        }
        String topic_name = this.topicMap.get(topic_id);
        logger.debug("Unregistering topic ID '" + topic_id + "': " + topic_name);
        this.topicMap.remove(topic_id);
    }
    
    private Short searchTopicId(String topic_name) throws MqttSnClientException {
    	for (Map.Entry<Short, String> entry : this.topicMap.entrySet()) {
            Short key = entry.getKey();
            String value = entry.getValue();
            if (topic_name.equals(value)) {
            	return key;
            }
        }
        return null;
    }
    
    private void sendPingReq() throws MqttSnClientException {
    	PingReqPacket pingReqPacket = new PingReqPacket();
        this.sendPacket(pingReqPacket.encode());
    }
    
    private void sendPacket(byte[] buf) throws MqttSnClientException {
    	DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length, address, this.port);
        try {
        	this.datagramSocket.setSoTimeout((int) this.getTimeoutMillis());
        	logger.debug("Sending " + MqttSnUtility.decodeType(buf[1])+ " packet...");
        	logger.debug("Send " + buf.length + " bytes: "+ HexUtils.bytesToHex(buf));
			datagramSocket.send(datagramPacket);
			
			// Store the last time that we sent a packet
			this.lastTransmit = System.currentTimeMillis();
	    } catch (IOException e) {
	    	throw new MqttSnClientException(e);
		}
    }
    
    private byte[] receivePacketSync() throws MqttSnClientException {
    	byte[] received = null;

        try {
        	byte[] buffer = new byte[MqttSnConstants.MAX_PACKET_LENGTH_EXTENDED];
        	this.datagramSocket.setSoTimeout((int) this.getTimeoutMillis());
        	DatagramPacket datagramPacket = new DatagramPacket(buffer, buffer.length);
        	datagramSocket.receive(datagramPacket);
	        if (datagramPacket.getData() == null) {
	        	return null;
	        }
	        int length = buffer[0];
	        received = new byte[length];
	        System.arraycopy(buffer, 0, received, 0, length);
	        
	        logger.debug("Received " + received.length + " bytes: " + HexUtils.bytesToHex(received));
	        logger.debug("Received " + MqttSnUtility.decodeType(received[1])+ " packet...");
	        
	        // Store the last time that we received a packet
	        this.lastReceive = System.currentTimeMillis();
	    } catch (IOException e) {
	    	throw new MqttSnClientException(e);
		}

        return received;
    }
    
    private byte[] receivePacketASync() throws MqttSnClientException {

        byte[] response = null;
		try {
		    Receiver receiver = new Receiver(this.datagramSocket, 1000);
		    ExecutorService executor = Executors.newSingleThreadExecutor();
	        Future<byte[]> future = executor.submit(receiver);
			response = future.get(1000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// Ignore
		} catch (ExecutionException e) {
			// Ignore
		} catch (TimeoutException e) {
			// Ignore
		}

        return response;
    }
    
    
 
	private void addMqttSnCallback(String topic, MqttSnListener aMqttSnCallback) {
		logger.debug("Store MqttSnCallback for topic " + topic);
		this.listOfMqttSnCallback.put(topic, aMqttSnCallback);
	}
	
	private void addMqttSnCallback(short topicID, MqttSnListener aMqttSnCallback) {
		logger.debug("Store MqttSnCallback for topic ID " + topicID);
		this.listOfMqttSnCallback.put(Short.toString(topicID), aMqttSnCallback);
	}
	
	private long getKeepAliveMillis() {
		return this.keepAlive * 1000;
	}
	
	private long getTimeoutMillis() {
		return this.timeout * 1000;
	}

	public void setKeepAlive(short value) {
        // Store the keep alive period
        if (value > 0) {
        	this.keepAlive = value;
        }
	}

	public void setTimeout(byte value) throws MqttSnClientException {
		this.timeout = value;
		if (this.connected) {
			try {
				this.datagramSocket.setSoTimeout((int) this.getTimeoutMillis());
			} catch (SocketException e) {
				throw new MqttSnClientException(e);
			}
		}
	}

	public void setClientID(String value) {
		if (value == null) {
			this.clientID = "mqtt-sn-java-" + (new Random().nextInt() & 0xffff);
		} else {
			this.clientID = value;
		}
	}

	public void setCleanSession(boolean value) {
		this.cleanSession = value;
	}

	public void setWill(String topic, String message, int qos, boolean retain) {
		this.setWill(topic, message.getBytes(), qos, retain);
	}
	
	public void setWill(String topic, byte[] message, int qos, boolean retain) {
		this.willTopic = topic;
		this.willMessage = message;
		this.willQos = qos;
		this.willRetain = retain;
	}
	
	public void setWillTopic(String value) {
		this.willTopic = value;
	}

	public void setWillMessage(String value) {
		this.setWillMessage(value.getBytes());
	}

	public void setWillMessage(byte[] value) {
		this.willMessage = value;
	}

	public void setWillQos(int value) {
		this.willQos = value;
	}

	public void setWillRetain(boolean value) {
		this.willRetain = value;
	}
	
	public static boolean isMatched(String topic, String topicFilter) {
        // Gestione dei casi in cui il topic o il topic filter siano vuoti
        if ((topic == null || topic.isEmpty()) && topicFilter.equals("#")) {
            return true; // Un topic vuoto corrisponde solo al filtro "#"
        }
        if (topic == null || topic.isEmpty() || topicFilter == null || topicFilter.isEmpty()) {
            return false; // Altri casi di topic o topic filter vuoti non corrispondono
        }

        // Dividi il topic e il topic filter in livelli
        String[] topicLevels = topic.split("/");
        String[] filterLevels = topicFilter.split("/");

        int topicLength = topicLevels.length;
        int filterLength = filterLevels.length;

        for (int i = 0; i < filterLength; i++) {
            // Controlla se siamo oltre la lunghezza del topic
            if (i >= topicLength) {
                // Restituisci true solo se il filtro termina con #
                return filterLevels[i].equals("#");
            }

            // Controlla il carattere jolly '+'
            if (filterLevels[i].equals("+")) {
                continue;
            }

            // Controlla il carattere jolly '#'
            if (filterLevels[i].equals("#")) {
                return true;
            }

            // Controlla la corrispondenza esatta
            if (!filterLevels[i].equals(topicLevels[i])) {
                return false;
            }
        }

        // Verifica che il topic non abbia livelli extra
        return topicLength == filterLength;
    }
	
	
	private HashMap<String, MqttSnListener> listOfMqttSnCallback = new HashMap<String, MqttSnListener>();
    
	private DatagramSocket datagramSocket;
	private boolean reuseAddress;
			
    private InetAddress address;
	private int port;
	private short keepAlive;
	private String clientID = null;
	private boolean cleanSession = true;
	
	private String willTopic;
	private byte[] willMessage;
	private int willQos;
	private boolean willRetain;
	
	private boolean connected;
	
	private int nextMessageID;

	private long lastTransmit;
	private long lastReceive;	
	private byte timeout;

	private HashMap<Short,String> topicMap = new HashMap<Short, String>();
	
	class Receiver implements Callable<byte[]> {
        private DatagramSocket socket;
        private int receiveTimeout;

        public Receiver(DatagramSocket socket, int receiveTimeout) {
            this.socket = socket;
            this.receiveTimeout = receiveTimeout;
        }

        @Override
        public byte[] call() {
        	byte[] received = null;
            try {
				socket.setSoTimeout(receiveTimeout);

				received = new byte[MqttSnConstants.MAX_PACKET_LENGTH_EXTENDED];
            	DatagramPacket datagramPacket = new DatagramPacket(received, received.length);
            	this.socket.receive(datagramPacket);
    	        if (datagramPacket.getData() == null) {
    	        	return null;
    	        }
    	        received = new String(datagramPacket.getData(), 0, datagramPacket.getLength()).getBytes();
    	        logger.debug("Received " + datagramPacket.getLength() + " bytes: " + HexUtils.bytesToHex(received));
    	        
    	        // Store the last time that we received a packet
    	        lastReceive = System.currentTimeMillis();
			} catch (SocketException e) {
				// Ignore
			} catch (SocketTimeoutException e) {
				// Ignore
            } catch (IOException e) {
            	// Ignore
			}
            return received;
        }
    }
}
