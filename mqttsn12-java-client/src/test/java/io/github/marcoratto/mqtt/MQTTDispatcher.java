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
package io.github.marcoratto.mqtt;

import java.util.Iterator;

import org.apache.commons.collections4.queue.CircularFifoQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import io.github.marcoratto.util.Utility;

public class MQTTDispatcher implements Runnable, MqttCallbackExtended {

	private final static Logger logger = LoggerFactory.getLogger(MQTTDispatcher.class);

	private static MQTTDispatcher instance = null;
	private static Thread thread = null;

	private MQTTUtility publisher;
	
	private boolean firstConnection = true;
	
	private CircularFifoQueue<RecordFifo> fifo = new CircularFifoQueue<RecordFifo>(1000);

	class RecordFifo {
		
		String topicName;
		int qos;
		boolean retained;
		String payload;
		
		public RecordFifo(String aTopic, int aQos, boolean aRetained, String aMessage) {
			this.topicName = aTopic;
			this.payload = aMessage;
			this.qos = aQos;
			this.retained = aRetained;
		}
		
	}
	
	private MQTTDispatcher() {
		logger.debug("MQTTDispatcher");
	}
	
	public void build(String prefix) {    	
    	this.publisher = new MQTTUtility();
    	this.publisher.setMqttCallbackExtended(this);
    	
    	this.init();
		
    	thread = new Thread(this);
    	thread.start();
		logger.info("Thread started.");
	}

	private synchronized void init() {
	
    	try {
        	this.publisher.connect(3);
        	if (this.firstConnection) {
        		synchronized (this) {
            		this.firstConnection = false;
            		logger.debug("Connected to the MQTT Broker: flush FIFO buffer...");
            		this.flushBuffer();					
				}
        	}
		} catch (MQTTException e) {
			logger.warn(e.getMessage());
		}		
	}

	  public static MQTTDispatcher getInstance() {
		    if (instance == null) {
			      synchronized(MQTTDispatcher.class) {
			    	  if (instance == null) {
				          instance = new MQTTDispatcher();
			    	  }
		    	}
		    }
		    return instance;
	  }
	  
	  public void reset() {
		  instance = null;
		  thread = null;
	  }	
	  
	    public void publish(String topicName, int qos, boolean retained, String payload) {
	    	
	    	synchronized (this) {
		    	if ((firstConnection == true) && 
		    		(this.publisher.isConnected() == false)) {
		    		this.fifo.add(new RecordFifo(topicName, qos, retained, payload));
		    		logger.warn("Never connected to MQTT Broker. Save message on a FIFO buffer.");
		    		return;
			    }				
			}
	    	try {
	    		this.publisher.publish(topicName, qos, retained, payload.getBytes());
	    	} catch (MQTTException e) {
				logger.warn(e.getMessage());
	    	}
	    }
	  
		@Override
		public void run() {
	
			while (true) {
				if (this.publisher.isConnected() == false) {
					logger.debug("MQTT connection lost! Try to reconnect...");
					this.init();
				}
				Utility.getInstance().sleepSeconds(1);
			}
			
		}

	    private synchronized void flushBuffer() {
	    	if (this.fifo.isEmpty()) {
				logger.warn("FIFO buffer is empty!");
				return;
			}
			logger.debug("Flush buffer...");
			RecordFifo record = null;
	    	try {
				if (this.publisher.isConnected() == false) {
					return;
				}
		    	Iterator<RecordFifo> iterator = this.fifo.iterator();
		    	while (iterator.hasNext()) {
		    		record = iterator.next();
		    		if (record != null) {
	    				this.publisher.publish(record.topicName, record.qos, record.retained, record.payload.getBytes());
	    	    	}
		    	}
			} catch (MQTTException e) {
				logger.warn(e.getMessage());
				logger.warn("No MQTT connection! Save message on FIFO queue.");
				this.fifo.add(record);
			}    			
	    }

		@Override
		public void connectionLost(Throwable cause) {
			logger.warn(cause.getMessage());
		}

		@Override
		public void deliveryComplete(IMqttDeliveryToken token) {
			logger.debug("deliveryComplete:" + token.getMessageId());									
		}

		@Override
		public void messageArrived(String topic, MqttMessage message) throws Exception {
		}

		@Override
		public void connectComplete(boolean reconnect, String serverURI) {
		}

}
