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
package io.github.marcoratto.junit.mqttsn;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import io.github.marcoratto.mqtt.MQTTUtility;
import io.github.marcoratto.mqttsn.MqttSnClient;
import io.github.marcoratto.mqttsn.MqttSnClientException;
import io.github.marcoratto.mqttsn.MqttSnConstants;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @author Marco Ratto
 *
 */
public class TestPublisher extends TestCase {
	
	private MqttSnClient mqttsnClient = null;
	private MQTTUtility  mqttu = null;
    
    private String actualMessage = null;
    private String actualWillMessage = null;
    
    private final static String MQTT_SN_HOST = "127.0.0.1";
    private final static int MQTT_SN_PORT = 2442;
    private final static String MQTT_SN_CLIENTID = "junit";
    
    private final static String MQTT_HOST = "127.0.0.1";
    private final static int MQTT_PORT = 1883;
      
	protected void setUp() {
		System.out.println(this.getName() + ".setUp()");	
		
        try {
        	mqttu = new MQTTUtility();
        	mqttu.setHostname(MQTT_HOST);
        	mqttu.setPort(MQTT_PORT);

        	mqttu.addListener("tt", 1, new TopicListener());
        	mqttu.addListener("mqttsn/sample", 1, new TopicListener());
        	for (int i = 0; i < 5; i++) {
        		mqttu.addListener("mqttsn/sample" + i, 1, new TopicListener());
        	}
        	mqttu.addListener("mqttsn/test/predefined_pub", 1, new TopicListener());
        	mqttu.addListener("mqttsn/status", 1, new WillTopicListener());
        	
        	mqttu.connect();
        	mqttu.subscribe();
        	
        	mqttsnClient = new MqttSnClient();
        	mqttsnClient.setClientID(MQTT_SN_CLIENTID);
        	mqttsnClient.setCleanSession(true);
        	mqttsnClient.setTimeout((byte) 60);
        	mqttsnClient.setKeepAlive((short) 30);
        	
        	this.actualMessage = null;
        	this.actualWillMessage = null;
        } catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
        }
	}

	protected void tearDown() {
		System.out.println(this.getName() + ".tearDown()");
		if (this.mqttu != null) {
			try {
				this.mqttu.disconnect();
			 } catch (Throwable t) {
				t.printStackTrace();
				fail(t.getMessage());
			}
			this.mqttu = null;
		}
	}

	public static void main (String[] args) {
		TestRunner.run(suite());
	}
	
	public static Test suite() {
		return new TestSuite(TestPublisher.class);
	}	

	public void testConnectDisconnect0() {
		System.out.println(this.getClass().getName() + ".testConnectDisconnect0()");
		try {						
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			
			Thread.sleep(1000);
			
			mqttsnClient.sendDisconnect((short) 0);
			
			mqttsnClient.close();
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	
	public void testSearchGateway() {
		System.out.println(this.getClass().getName() + ".testSearchGateway()");
		try {						
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			
			mqttsnClient.sendSearchGateway((byte) 3);
			
			Thread.sleep(500);
			
			mqttsnClient.sendDisconnect((short) 0);
			
			mqttsnClient.close();
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	
	public void testPublishTopicPredefined_Qos_N1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishTopicPredefined_Qos_N1()");
		try {					
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendPublish((short) 1, 
								MqttSnConstants.TOPIC_TYPE_PREDEFINED, 
								expected.getBytes(), 
								MqttSnConstants.QOS_N1, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testClientIdNull() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testClientIdNull()");
		try {					
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttsnClient.setClientID(null);
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendPublish((short) 1, 
								MqttSnConstants.TOPIC_TYPE_PREDEFINED, 
								expected.getBytes(), 
								MqttSnConstants.QOS_N1, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testLWT1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testLWT1()");
		try {		
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.setWillMessage("offline");
			this.mqttsnClient.setWillTopic("mqttsn/status");
			this.mqttsnClient.setWillQos(1);
			this.mqttsnClient.setWillRetain(true);
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_0, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testLWT2() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testLWT2()");
		try {		
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.setWill("mqttsn/status", "offline", MqttSnConstants.QOS_1, true);
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_0, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testLWTUpdateMessage() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testLWTUpdateMessage()");
		try {		
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.setWill("mqttsn/status", "offline", MqttSnConstants.QOS_1, true);
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			
			Thread.sleep(1000);
			
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_0, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendWillMessageUpdate("off");
			
			Thread.sleep(1000);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testLWTUpdateTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testLWTUpdateTopic()");
		try {		
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.setWill("mqttsn/status", "offline", MqttSnConstants.QOS_1, true);
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			
			Thread.sleep(1000);
			
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_0, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			this.mqttsnClient.setWillTopic("mqttsn/state");
			mqttsnClient.sendWillTopicUpdate();
			
			Thread.sleep(1000);
			
			mqttsnClient.sendDisconnect();
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishNormalTopicWithQos2() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishNormalTopicWithQos2()");
		try {		
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			
			mqttsnClient.sendConnect();
			
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_2, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishNormalTopicWithQos0() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishNormalTopicWithQos0()");
		try {		
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			
			mqttsnClient.sendConnect();
			
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_0, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishShortTopicQosN1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishShortTopicQosN1()");
		try {			
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendPublish("tt".getBytes(), 
					expected.getBytes(), 
					MqttSnConstants.QOS_N1, 
					false);

			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishTopicPredefinedQosN1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishTopicPredefinedQosN1()");
		try {			
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			
			mqttsnClient.sendPublish((short) 1, 
					MqttSnConstants.TOPIC_TYPE_PREDEFINED,
					expected.getBytes(),
					MqttSnConstants.QOS_N1, 
					false);

			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			
			mqttsnClient.close();
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishLongTopicQos1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishLongTopicQos1()");
		try {			
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_1, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishBigPayload() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishLongTopicQos1()");
		try {			
			String expected = "";
			for (int j=0; j <5000; j++) {
				expected += "Hello " + (new Random().nextInt() & 0xffff);
			}
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			mqttsnClient.sendPublish("mqttsn/sample", 
								expected.getBytes(), 
								MqttSnConstants.QOS_1, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testRegisterAndPublishLongTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testRegisterAndPublishLongTopic()");
		try {			
			short[] listOfTopicID = new short[5];
			String[] listOfTopic = new String[5];
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			
			for (int i = 0; i < listOfTopic.length; i++) {
				String topic = "mqttsn/sample" + i;
				listOfTopicID[i] = mqttsnClient.sendRegister(topic);
				listOfTopic[i] = topic;
			}
			
			for (int i = 0; i < listOfTopicID.length; i++) {
				String expected = "Hello " + (new Random().nextInt() & 0xffff);
				mqttsnClient.sendPublish(listOfTopicID[i],
						MqttSnConstants.TOPIC_TYPE_NORMAL,
									expected.getBytes(), 
									MqttSnConstants.QOS_1, 
									false);
				
				Thread.sleep(1000);
				
				assertEquals(expected, this.actualMessage);
			}
						
			mqttsnClient.sendDisconnect();
			mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPublishLongTopicTooLong() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPublishLongTopicTooLong()");
		try {			
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			mqttsnClient.sendConnect();
			mqttsnClient.sendPublish("mqttsn/sample/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890/1234567890", 
								expected.getBytes(), 
								MqttSnConstants.QOS_0, 
								false);
			
			Thread.sleep(1000);
			
			assertEquals(expected, this.actualMessage);
			
			mqttsnClient.sendDisconnect((short) 0);
			mqttsnClient.close();
		} catch (MqttSnClientException e) {
			assertEquals("Topic name is too long", e.getMessage());
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	class TopicListener implements IMqttMessageListener { 

		ExecutorService pool = Executors.newFixedThreadPool(10);

		  class MessageHandler implements Runnable {

		    public MessageHandler(String topic, MqttMessage message) {
				actualMessage = new String(message.getPayload());
				System.out.println("TopicListener(): Ricevuto payload: " + actualMessage + " sul topic: " + topic);
		    }

		    public void run() {
		      //process message
		    }
		  }

		  @Override
		  public void messageArrived(String topic, MqttMessage message) throws Exception {
		    pool.execute(new MessageHandler(topic,message));
		  }
	}
	
	class WillTopicListener implements IMqttMessageListener { 

			  ExecutorService pool = Executors.newFixedThreadPool(10);

			  class MessageHandler implements Runnable {

			    public MessageHandler(String topic, MqttMessage message) {
					actualWillMessage = new String(message.getPayload());
					System.out.println("WillTopicListener(): Ricevuto payload: " + actualWillMessage + " sul topic: " + topic);
			    }

			    public void run() {
			      //process message
			    }
			  }

			  @Override
			  public void messageArrived(String topic, MqttMessage message) throws Exception {
			    pool.execute(new MessageHandler(topic, message));
			  }
	}
			
}
