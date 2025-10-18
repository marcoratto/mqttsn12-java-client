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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

import io.github.marcoratto.mqttsn.util.HexUtils;
import io.github.marcoratto.util.MyProperties;
import io.github.marcoratto.util.Utility;

public class MQTTUtility {
	
	private final static Logger logger = LoggerFactory.getLogger(MQTTUtility.class);
		
	private String hostname 	 = "127.0.0.1";
	private int port 			 = 1883;
	private String clientId 	 = null;
	private boolean cleanSession = true;			// Non durable subscriptions
	private boolean ssl 		 = false;
	private boolean automaticReconnect = false;
	private String password;
	private String userName;	
	
	private String 				url;
	private MqttClient			client;
	private MqttConnectOptions 	conOpt;
	
	private String willTopic;
	private byte[] willPayload;
	private int willQOS	= 0;
	private boolean willRetained = true;
	private boolean persistence = false;	
	private int connectionTimeout = 30;	
	private int keepAliveInterval = 60;	
	private int maxReconnectDelay = 5;
	private int maxInflight = 100;
	
	private boolean connected = false;
	
	private MqttCallbackExtended mqttCallbackExtended;
	private MqttCallback mqttCallback;

	private MqttClientPersistence dataStore;

	private String persistenceFolder;
	
	private ExecutorService executor;
	
	private Vector<MQTTSubscriber> listOfListener;
	
	public MQTTUtility() {
		this.executor = Executors.newFixedThreadPool(10);
		this.listOfListener = new Vector<MQTTSubscriber>(0, 1);
	}
	
	public MQTTUtility(MyProperties props, String prefix) {		
		this();
		
		this.setMaxInflight(props.getProperty(prefix + ".maxInflight", 10));
		
		this.setAutomaticReconnect(props.getProperty(prefix + ".automaticReconnect", false));
		
		this.setPersistence(props.getProperty(prefix + ".persistence", false));
		this.setPersistenceFolder(props.getProperty(prefix + ".persistenceFolder", System.getProperty("java.io.tmpdir")));
		
		this.setHostname(props.getProperty(prefix + ".host", null));
		this.setPort(props.getProperty(prefix + ".port", 1883));
		this.setUserName(props.getProperty(prefix + ".user", null));
		this.setPassword(props.getProperty(prefix + ".pass", null));
		this.setClientId(props.getProperty(prefix + ".clientId", null));
		this.setCleanSession(props.getProperty(prefix + ".cleanSession", true));
		this.setSsl(props.getProperty(prefix + ".ssl", false));
		
		this.setWillTopic(props.getProperty(prefix + ".willTopic", null));
		this.setWillPayload(props.getProperty(prefix + ".willPayload", null));
		this.setWillQOS(props.getProperty(prefix + ".willQOS", 2));		
		this.setWillRetained(props.getProperty(prefix + ".willRetained", true));
		
		this.setConnectionTimeout(props.getProperty(prefix + ".connectionTimeout", 30));
		this.setKeepAliveInterval(props.getProperty(prefix + ".keepAliveInterval", 30));
		this.setMaxReconnectDelay(props.getProperty(prefix + ".maxReconnectDelay", 5));
	}

	private void init() {
		if (this.clientId == null) {
	        String username =  System.getProperty("user.name");
	        String hostname = "";
	        try {
	            hostname = InetAddress.getLocalHost().getHostName();
	        } catch (UnknownHostException e) {
	        	logger.warn(e.getMessage());
	        }
	        this.clientId = username + "@" + hostname;	
	        logger.debug("clientId is null! Forced to " + this.clientId);
		}
	}
	
	public void connect() throws MQTTException {
		this.connect(1);
	}
		
	public void connect(int retry) throws MQTTException {
		if (this.isConnected()) {
			logger.warn("already connected.");
			return;
		}
    	
    		this.config();
    		
        	if (this.mqttCallbackExtended != null) {
        		logger.debug("mqttCallbackExtended configured.");
            	client.setCallback(this.mqttCallbackExtended);        		        		
        	}
        	
        	if (this.mqttCallback != null) {
        		logger.debug("mqttCallback configured.");
            	client.setCallback(this.mqttCallback);        		        		
        	}
        	
        	while (retry > 0) {
        		try {
        			// Connect to the MQTT server
        			logger.debug("Try to connect (" + retry + ")");
        			this.client.connect(this.conOpt);
        			if (this.client.isConnected() == false) {
        				Utility.getInstance().sleepSeconds(1);
        			} else {
        				retry=0;
        			}
	    	    } catch (MqttException e) {
	    	    	retry--;
	    			if (retry == 0) {
	    				throw new MQTTException(e);
	    			}           	
	    	    }
        	}
     
        	logger.debug("Connected to '" + url + "' with client ID '" + client.getClientId() + "'");
        	this.connected = true;
	}
	
	public void disconnect() throws MQTTException {		
		if (client != null) {
	    	// Disconnect the client
	    	try {
	    		this.client.disconnect();
			} catch (MqttException e) {
				logger.warn("Ignored:" + e.getMessage());
			}
		}		
    	logger.debug("Disconnected");			
    	connected = false;
	}

    public boolean isConnected() {
		if (this.client == null) {
			return false;
		}
		if (this.connected == false) {
			return false;
		}
		return this.client.isConnected();
    }    
    
    public void publish(String topicName, int qos, boolean retained, byte[] payload) throws MQTTException {
    	if (this.connected == false) {
    		logger.warn("Not connected to the Broker!");
    		return;
    	}
    	MqttMessage message = null;
    	try {

	    	// Create and configure a message
	   		message = new MqttMessage(payload);
	   		message.setQos(qos);
	    	message.setRetained(retained);

	    	logger.debug("Send message of " + message.getPayload().length + " bytes to topic '" + topicName + "',QOS=" + message.getQos() + ",retained=" + message.isRetained());
	    	this.client.publish(topicName, message);		    	
	    	logger.debug("Message sent");
	    } catch (MqttSecurityException e) {
			logger.warn(e.getMessage());
	    } catch (MqttPersistenceException e) {
	    	logger.warn(e.getMessage());
	    } catch (MqttException e) {
	    	logger.warn(e.getMessage());
		} 	

    }
    
    public void publishBackground(String topicName, int qos, boolean retained, byte[] payload) throws MQTTException {
    	if (this.connected) {
    		this.executor.execute(new Publisher(this.client, topicName, qos, retained, payload));	
    	} else {
    		logger.warn("Not connected to the Broker!");
    	} 	
    }

    /*
     Send&Forget a message on a Topic using a retry Exponential Backoff Algorithm.
     * */
    public void sendAndForget(int retry, String topicName, int qos, boolean retained, String payload) throws MQTTException {
    	// Both connect and publish operations may fail. If they do, allow retries but with an
        // exponential backoff time period.
        long initialConnectIntervalMillis = 500L;
        long maxConnectIntervalMillis = 6000L;
        float intervalMultiplier = 1.5f;

        long retryIntervalMs = initialConnectIntervalMillis;
        
    	while (retry > 0) {
    		try {
    			this.sendAndForget(topicName, qos, retained, payload);
    			retry = 0;
    		} catch (MQTTException e) {
    			retry--;
    			if (retry == 0) {
    				throw e;
    			}

    	        // If the connection is lost or if the server cannot be connected, allow retries, but with
    	        // exponential backoff.
    	          logger.debug("Retrying in " + retryIntervalMs / 1000.0 + " seconds.");
    	          retryIntervalMs *= intervalMultiplier;
    	          if (retryIntervalMs > maxConnectIntervalMillis) {
    	             retryIntervalMs = maxConnectIntervalMillis;
    	          }    			
    	          Utility.getInstance().sleepMilliseconds(retryIntervalMs);
    		}
    	}
    }
    
    public void sendAndForget(String topicName, int qos, boolean retained, String payload) throws MQTTException {
    	try {
    		this.connect(3);
    		
	    	// Create and configure a message
	   		MqttMessage message = new MqttMessage(payload.getBytes());
	    	message.setQos(qos);
	    	message.setRetained(retained);

	    	client.publish(topicName, message);
	    	logger.debug("Send message of " + payload.length() + " bytes to topic '" + topicName + "',QOS=" + qos + ",retained=" + retained);
	    } catch (MqttSecurityException e) {
			throw new MQTTException(e);
		} catch (MqttException e) {
			throw new MQTTException(e);
		} finally {
			this.disconnect();
		}		
    }
    
    public void addListener(String topicFilter, int qos, IMqttMessageListener aMQTTMessageListener)  {
    	if ((topicFilter != null) && (aMQTTMessageListener != null)) {
        	MQTTSubscriber mqttSubscriber = new MQTTSubscriber();
        	mqttSubscriber.listener = aMQTTMessageListener;
        	mqttSubscriber.qos = qos;
        	mqttSubscriber.topicFilter = topicFilter;
        	this.listOfListener.add(mqttSubscriber);    		
    	}
    }
	
	public void subscribe(String topicFilter, int qos, MqttCallback aMqttCallback) throws MQTTException {
		try {
			this.client.setCallback(aMqttCallback);
			this.client.subscribe(topicFilter, qos);
						
			logger.debug("Subscribed on topicFilter '" + topicFilter + "' with QOS=" + qos);
		} catch (MqttException e) {
			throw new MQTTException(e);
		}
	}
	
	
	public void subscribe(String[] listOfTopicFilters, int[] listOfQos, IMqttMessageListener[] listOfMQTTMessageListener) throws MQTTException {
		if (listOfTopicFilters.length != listOfQos.length) {
			throw new MQTTException("ERROR: list of Topic Filters different from list of QOS! Stopped.");
		}
		if (listOfTopicFilters.length != listOfMQTTMessageListener.length) {
			throw new MQTTException("ERROR: list of Topic Filters different from list of Message Listener! Stopped.");
		}
		try {
    		for (int j=0; j < listOfTopicFilters.length; j++) {
    			logger.debug("Subscribing on topicFilter '" + listOfTopicFilters[j] + "' with QOS=" + listOfQos[j]);    			
    		}    		
			this.client.subscribe(listOfTopicFilters, listOfQos, listOfMQTTMessageListener);
						
		} catch (MqttException e) {
			throw new MQTTException(e);
		}
	}
	
	public void subscribe() throws MQTTException {
		this.subscribe(this.listOfListener);
	}
	
	public void subscribe(Vector<MQTTSubscriber> listOfListener) throws MQTTException {
		if (listOfListener == null) {
			throw new MQTTException("ERROR: why param 'listOfListener' is null ?");
		}
		if (listOfListener.size() == 0) {
			logger.warn("listOfListener is empty!");
			return;
		}
		String[] listOfTopicFilters = new String[listOfListener.size()];
		int[] listOfQos = new int[listOfListener.size()];
		IMqttMessageListener[] listOfMQTTMessageListener = new IMqttMessageListener[listOfListener.size()];
		try {
    		for (int j=0; j < listOfListener.size(); j++) {
    			String topicFilter = listOfListener.get(j).topicFilter;
    			int qos = listOfListener.get(j).qos;
    			IMqttMessageListener listener = listOfListener.get(j).listener;
    			listOfTopicFilters[j] = topicFilter;
    			listOfQos[j] = qos;
    			listOfMQTTMessageListener[j] = listener;
    			logger.debug("Subscribing on topicFilter '" + listOfListener.get(j).topicFilter + "' with QOS=" + listOfQos[j]);    			
    		}
    		
    		this.client.subscribe(listOfTopicFilters, listOfQos, listOfMQTTMessageListener);
		} catch (MqttException e) {
			throw new MQTTException(e);
		}
	}
	
	private void config() throws MQTTException {
		this.init();
		
		String protocol = "tcp://";

	    if (this.ssl) {
	      protocol = "ssl://";
	    }

	    this.url = protocol + hostname + ":" + port;
	    logger.debug("url is " + this.url);
		
    	if (this.persistence) {
    		logger.debug("persistence enabled on filesystem");
    		this.dataStore = new MQTTFileCache(this.persistenceFolder);    		
    	} else {    		
    		// dataStore = new MemoryPersistence();
    		logger.debug("persistence enabled on memory");
    		this.dataStore = new MQTTMemoryCache();
    	}
		
		// Construct the connection options object that contains connection parameters
		// such as cleanSession and LWT
    	this.conOpt = new MqttConnectOptions();
    	this.conOpt.setCleanSession(this.cleanSession);    	    	
    	this.conOpt.setAutomaticReconnect(this.automaticReconnect);
    	this.conOpt.setConnectionTimeout(this.connectionTimeout);
    	this.conOpt.setKeepAliveInterval(this.keepAliveInterval);
    	this.conOpt.setMaxReconnectDelay(this.maxReconnectDelay);
    	this.conOpt.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);    	
    	this.conOpt.setMaxInflight(this.maxInflight);

    	if(this.password != null ) {
    	  logger.debug("password enabled");
    	  this.conOpt.setPassword(this.password.toCharArray());
    	}
    	if(this.userName != null) {
    	  logger.debug("userName enabled");
    	  this.conOpt.setUserName(this.userName);
    	}
    	
    	if ((this.willTopic != null) && 
    		(this.willPayload != null)) { 
    		logger.debug("LWT enabled");
    		this.conOpt.setWill(this.willTopic, this.willPayload, this.willQOS, this.willRetained);
    	} else {
    		logger.debug("LWT disabled");
    	}
    	
        // Construct an MQTT blocking mode client
		try {
			logger.info("Trying to connect to '" + this.url + "' with clientId=" + clientId);
	    	
			this.client = new MqttClient(this.url, this.clientId, this.dataStore);

			// client.setTimeToWait(this.timeToWait);
			
		} catch (MqttException e) {			
			throw new MQTTException(e);
		}		
	}
	
	public String getHostname() {
		return hostname;
	}

	public void setHostname(String value) {
		this.hostname = value;
		logger.debug("hostname is " + this.hostname);
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
		logger.debug("port is " + this.port);
	}

	/*
	 * Default: <username>@<hostname> 
	 */
	public String getClientId() {
		return clientId;
	}

	/*
	 * Default: <username>@<hostname> 
	 * If null not updated the value
	 */
	public void setClientId(String value) {
		if (value != null) {
			this.clientId = value;
		}
		logger.debug("clientId is " + this.clientId);
	}
	
	public void setClientId(int value) {
		this.clientId = String.valueOf(value);
		logger.debug("clientId is " + this.clientId);
	}	

	public void setClientId() {
		this.clientId = MqttClient.generateClientId();
		logger.debug("clientId is " + this.clientId);
	}

	public boolean isCleanSession() {
		return cleanSession;
	}

	public void setCleanSession(boolean value) {
		this.cleanSession = value;
		logger.debug("cleanSession is " + this.cleanSession);
	}

	public boolean isSsl() {
		return ssl;
	}

	public void setSsl(boolean value) {
		this.ssl = value;
		logger.debug("ssl is " + this.ssl);
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String value) {
		this.password = value;
		logger.debug("password is " + this.password);
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String value) {
		this.userName = value;
		logger.debug("userName is " + this.userName);
	}

	public String getWillTopic() {
		return willTopic;
	}

	public void setWillTopic(String value) {
		this.willTopic = value;
		logger.debug("willTopic is " + this.willTopic);
	}

	public String getWillPayload() {
		return willPayload.toString();
	}

	public void setWillPayload(String value) {
		if (value != null) {
			this.willPayload = value.getBytes();	
		}
		logger.debug("willPayload is " + this.willPayload);
	}
	
	public void setWillPayload(byte[] value) {
		this.willPayload = value;
		logger.debug("willPayload is " + HexUtils.bytesToHex(this.willPayload));
	}

	public int getWillQOS() {
		return willQOS;
	}

	public void setWillQOS(int value) {
		this.willQOS = value;
		logger.debug("willQOS is " + this.willQOS);
	}

	public boolean isWillRetained() {
		return willRetained;
	}

	public void setWillRetained(boolean value) {
		this.willRetained = value;
		logger.debug("willRetained is " + this.willRetained);
	}

	public boolean getAutomaticReconnect() {
		return automaticReconnect;
	}

	public void setAutomaticReconnect(boolean value) {
		this.automaticReconnect = value;
		logger.debug("automaticReconnect is " + this.automaticReconnect);
	}

	public boolean getPersistence() {
		return persistence;
	}

	public void setPersistence(boolean value) {
		this.persistence = value;
		logger.debug("persistence is " + this.persistence);
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int value) {
		this.connectionTimeout = value;
		logger.debug("connectionTimeout is " + this.connectionTimeout);
	}

	public int getKeepAliveInterval() {
		return keepAliveInterval;
	}

	public void setKeepAliveInterval(int value) {
		this.keepAliveInterval = value;
		logger.debug("keepAliveInterval is " + this.keepAliveInterval);
	}

	public int getMaxReconnectDelay() {
		return maxReconnectDelay;
	}

	public void setMaxReconnectDelay(int value) {
		this.maxReconnectDelay = value;
		logger.debug("maxReconnectDelay is " + this.maxReconnectDelay);
	}
		
	public int getMaxInflight() {
		return maxInflight;
	}

	public void setMaxInflight(int value) {
		this.maxInflight = value;
		logger.debug("maxInflight is " + this.maxInflight);
	}

	public MqttCallbackExtended getMqttCallbackExtended() {
		return mqttCallbackExtended;
	}

	public void setMqttCallbackExtended(MqttCallbackExtended value) {
		this.mqttCallbackExtended = value;
		logger.debug("mqttCallbackExtended is " + this.mqttCallbackExtended);
	}

	public String getPersistenceFolder() {
		return persistenceFolder;
	}

	public void setPersistenceFolder(String value) {
		this.persistenceFolder = value;
		logger.debug("persistenceFolder is " + this.persistenceFolder);
	}

	public MqttCallback getMqttCallback() {
		return mqttCallback;
	}

	public void setMqttCallback(MqttCallback mqttCallback) {
		this.mqttCallback = mqttCallback;
	}
	
	private class Publisher implements Runnable {

		private MqttClient client = null;
		private String topic;
		private byte[] payload;
		private int qos = 0;
		private boolean retained = false;

		public Publisher(MqttClient client, String topicName, int qos, boolean retained, byte[] payload) {
			this.client = client;
			this.topic = topicName;
			this.payload = payload;
			this.qos = qos;
			this.retained = retained;
		}

		public void run() {
	    	MqttMessage message = null;
	    	try {

		    	// Create and configure a message
		   		message = new MqttMessage(payload);
		    	message.setQos(qos);
		    	message.setRetained(retained);

		    	this.client.publish(topic, message);
		    	
	    		logger.debug("Send message of " + payload.length + " bytes to topic '" + topic + "',QOS=" + qos + ",retained=" + retained);
		    	
		    } catch (MqttSecurityException e) {
				logger.warn(e.getMessage());
		    } catch (MqttPersistenceException e) {
		    	logger.warn(e.getMessage());
		    } catch (MqttException e) {
		    	logger.warn(e.getMessage());
			} 
		}
	}

}
