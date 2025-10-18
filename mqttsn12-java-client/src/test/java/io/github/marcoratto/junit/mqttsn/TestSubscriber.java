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

import io.github.marcoratto.mqtt.MQTTUtility;
import io.github.marcoratto.mqttsn.MqttSnClient;
import io.github.marcoratto.mqttsn.MqttSnClientException;
import io.github.marcoratto.mqttsn.MqttSnConstants;
import io.github.marcoratto.mqttsn.MqttSnListener;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @author Marco Ratto
 *
 */
public class TestSubscriber extends TestCase implements MqttSnListener {
	
	private MqttSnClient mqttsnClient = null;
	private MQTTUtility  mqttu = null;
    
    private String actual = null;
    
    private final static String MQTT_SN_HOST = "127.0.0.1";
    private final static int MQTT_SN_PORT = 2442;
    private final static String MQTT_SN_CLIENTID = "junit";
    
    private final static String MQTT_HOST = "127.0.0.1";
    private final static int MQTT_PORT = 1883;

	protected void setUp() {
		System.out.println(this.getName() + ".setUp()");	
		
        try {
        	this.mqttu = new MQTTUtility();
        	this.mqttu.setHostname(MQTT_HOST);
        	this.mqttu.setPort(MQTT_PORT);
        	this.mqttu.connect();
        	
        	this.mqttsnClient = new MqttSnClient();
        	this.mqttsnClient.setClientID(MQTT_SN_CLIENTID);
        	this.mqttsnClient.setCleanSession(true);
        	
        	this.actual = null;
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
		}
	}

	public static void main (String[] args) {
		TestRunner.run(suite());
	}
	
	public static Test suite() {
		return new TestSuite(TestSubscriber.class);
	}	

	public void testSubscribeNormalTopicQos0() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeNormalTopicQos0()");
		try {						
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			boolean keep_running = true;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/sub", MqttSnConstants.QOS_0, this);
			
			this.mqttu.publish("mqttsn/sub", 0, false, expected.getBytes());
			
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testSubscribeNormalTopicQos1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeNormalTopicQos1()");
		try {						
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			boolean keep_running = true;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/sub", MqttSnConstants.QOS_1, this);
			
			this.mqttu.publish("mqttsn/sub", 1, false, expected.getBytes());
			
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testPing() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testPing()");
		try {						
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			boolean keep_running = true;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/sub", MqttSnConstants.QOS_1, this);
			
			for(int j=0; j <20; j++) {
				mqttsnClient.polling();
				Thread.sleep(1000);
			}
			
			this.mqttu.publish("mqttsn/sub", 1, false, expected.getBytes());
			
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testUnSubscribeNormalTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testUnSubscribeNormalTopic()");
		try {						
	
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/sub", MqttSnConstants.QOS_1, this);
			
			Thread.sleep(1000);
			this.mqttsnClient.sendUnSubscribe("mqttsn/sub");
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testUnSubscribeShortTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testUnSubscribeShortTopic()");
		try {						
	
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("rm", MqttSnConstants.QOS_1, this);
			
			Thread.sleep(1000);
			this.mqttsnClient.sendUnSubscribe("rm");
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testUnSubscribePredefinedTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testUnSubscribeShortTopic()");
		try {						
	
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe((short) 2, MqttSnConstants.QOS_1, this);
			
			Thread.sleep(1000);
			this.mqttsnClient.sendUnSubscribe((short) 2);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testSubscribeBigPayload() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeBigPayload()");
		try {						
			String expected = "";
			for (int j=0; j <10; j++) {
				expected += "Hello " + (new Random().nextInt() & 0xffff);
			}
			boolean keep_running = true;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/sub/bigpayload", MqttSnConstants.QOS_1, this);
			
			Thread.sleep(500);
			
			this.mqttu.publish("mqttsn/sub/bigpayload", 1, false, expected.getBytes());
			
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testSubscribeShortTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeShortTopic()");
		try {						
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			boolean keep_running = true;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("tt", MqttSnConstants.QOS_1, this);
			
			Thread.sleep(1000);
			
			this.mqttu.publish("tt", 1, false, expected.getBytes());

	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testSubscribePredefinedTopic() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeShortTopic()");
		try {						
			String expected = "Hello " + (new Random().nextInt() & 0xffff);
			boolean keep_running = true;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe((short) 2, MqttSnConstants.QOS_1, this);
			
			Thread.sleep(1000);
			
			this.mqttu.publish("mqttsn/test/predefined_sub", 1, false, expected.getBytes());

	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testSubscribeMultipleLongTopicQos1() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeMultipleLongTopicQos1()");
		try {						
			String expected;
			boolean keep_running;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/subscribe/1", MqttSnConstants.QOS_1, this);
			this.mqttsnClient.sendSubscribe("mqttsn/subscribe/2", MqttSnConstants.QOS_1, this);
			this.mqttsnClient.sendSubscribe("mqttsn/subscribe/3", MqttSnConstants.QOS_1, this);
			
			expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttu.publish("mqttsn/subscribe/1", 1, false, expected.getBytes());
			
			keep_running = true;
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttu.publish("mqttsn/subscribe/2", 1, false, expected.getBytes());
			
			keep_running = true;
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttu.publish("mqttsn/subscribe/3", 1, false, expected.getBytes());
			
			keep_running = true;
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	public void testSubscribeWildcards() throws MqttSnClientException {
		System.out.println(this.getClass().getName() + ".testSubscribeWildcards()");
		try {						
			String expected;
			boolean keep_running;
			
			this.mqttsnClient.open(MQTT_SN_HOST, MQTT_SN_PORT);
			this.mqttsnClient.sendConnect();
			this.mqttsnClient.sendSubscribe("mqttsn/subscribe/+", MqttSnConstants.QOS_1, this);
			this.mqttsnClient.sendSubscribe("mqttsn/#", MqttSnConstants.QOS_1, this);
			Thread.sleep(500);
			
			expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttu.publish("mqttsn/subscribe/1", 1, false, expected.getBytes());
			
			keep_running = true;
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        Thread.sleep(500);
	        expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttu.publish("mqttsn/subscribe/2", 1, false, expected.getBytes());
			
			keep_running = true;
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        Thread.sleep(500);
	        expected = "Hello " + (new Random().nextInt() & 0xffff);
			this.mqttu.publish("mqttsn/subscribe/3", 1, false, expected.getBytes());
			
			keep_running = true;
	        while(keep_running) {
	        	mqttsnClient.polling();
	        	if (this.actual.equalsIgnoreCase(expected)) {
	        		keep_running = false;
	        	}
			}
	        assertEquals(this.actual, expected);
	        
	        mqttsnClient.sendDisconnect((short) 0);
	        mqttsnClient.close();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
		@Override
		public void messageArrived(short topicID, String topicName, byte[] message) throws MqttSnClientException {
			this.actual = new String(message);
			System.out.println("Ricevuto payload: " + actual);
		}

}
