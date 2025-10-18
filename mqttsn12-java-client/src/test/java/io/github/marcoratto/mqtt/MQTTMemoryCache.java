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

import java.util.Enumeration;
import java.util.Hashtable;

import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttPersistable;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

public class MQTTMemoryCache implements MqttClientPersistence {

	private Hashtable<String, MqttPersistable> data;
	
	public MQTTMemoryCache() {
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#close()
	 */
	public void close() throws MqttPersistenceException {
		if (data != null) {
			data.clear();
		}	
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#keys()
	 */
	public Enumeration<String> keys() throws MqttPersistenceException {
		checkIsOpen();
		return data.keys();
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#get(java.lang.String)
	 */
	public MqttPersistable get(String key) throws MqttPersistenceException {
		checkIsOpen();
		return (MqttPersistable)data.get(key);
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#open(java.lang.String, java.lang.String)
	 */
	public void open(String clientId, String serverURI) throws MqttPersistenceException {
		this.data = new Hashtable<String, MqttPersistable>();
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#put(java.lang.String, org.eclipse.paho.mqttv5.client.MqttPersistable)
	 */
	public void put(String key, MqttPersistable persistable) throws MqttPersistenceException {
		checkIsOpen();
		data.put(key, persistable);
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#remove(java.lang.String)
	 */
	public void remove(String key) throws MqttPersistenceException {
		checkIsOpen();
		data.remove(key);
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#clear()
	 */
	public void clear() throws MqttPersistenceException {
		checkIsOpen();
		data.clear();
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttClientPersistence#containsKey(java.lang.String)
	 */
	public boolean containsKey(String key) throws MqttPersistenceException {
		checkIsOpen();
		return data.containsKey(key);
	}
	
	/**
	 * Checks whether the persistence has been opened.
	 * @throws MqttPersistenceException if the persistence has not been opened.
	 */
	private void checkIsOpen() throws MqttPersistenceException {
		if (data == null) {
			throw new MqttPersistenceException();
		}
	}

}
