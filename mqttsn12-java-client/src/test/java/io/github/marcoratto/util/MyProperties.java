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
package io.github.marcoratto.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyProperties {

	private final static Logger logger = LoggerFactory.getLogger(MyProperties.class);
	
	private static MyProperties instance = null;
	
	private Properties properties = null;

	private File propertiesFile;
	
	private MyProperties() {
		this.properties = new Properties();
	}

	  public void reset() {
		  instance = null;
	  }
	  
	  public static MyProperties getInstance() {
	    if (instance == null) {
	      synchronized(MyProperties.class) {
	        if (instance == null) {
	          instance = new MyProperties();
	        }
	      }
	    }
	    return instance;
	  }
	  
	  public final  String getProperty(String key, String defaultValue) {
			String value = this.properties.getProperty(key, defaultValue);
			logger.trace(key + "=" + value);
			return value;
	  }
		  
	  public final int getProperty(String key, int defaultValue) {
		    return Integer.parseInt(this.getProperty(key, String.valueOf(defaultValue)), 10);
	  }
	  
	  public final boolean getProperty(String key, boolean defaultValue) {
		    return this.getProperty(key, String.valueOf(defaultValue)).equalsIgnoreCase("true");
	  } 	

	  public final int[] getProperty(String key) {
		  String str = this.getProperty(key, null);
		  if (str == null) {
			  return null;
		  }
		  String[] tokens = str.split(",");
		  int[] numbers = new int[tokens.length];
		  for (int i = 0; i < tokens.length; i++) {
		      numbers[i] = Integer.parseInt(tokens[i]);
		  }
		  return numbers;
	  }
	  
	  public void readFileProperties(File f) throws MyPropertiesException {
	  	this.propertiesFile = f;
	    this.readFileProperties();
	  }
		 
      public void readFileProperties() throws MyPropertiesException {
			if (this.propertiesFile == null) {
				throw new MyPropertiesException("ERROR: Parameter is null! Stopped.");				
			}	
		  	if (this.propertiesFile.exists() == false) {
				throw new MyPropertiesException("ERROR: File '" + this.propertiesFile.getAbsolutePath() + "' not found! Stopped.");				
			}		
		    InputStream is = null;
		    this.properties = new Properties();
		    try {
				is = new FileInputStream(this.propertiesFile);
				this.properties.load(is);
			} catch (FileNotFoundException e) {
				throw new MyPropertiesException(e);
			} catch (IOException e) {
				throw new MyPropertiesException(e);
			}    
	  }
	  
	  public void saveFileProperties(File f) throws MyPropertiesException {
		  this.propertiesFile = f;
		  this.saveFileProperties();
	  }
	  
	  public void saveFileProperties() throws MyPropertiesException {
		  	if (this.propertiesFile == null) {
				throw new MyPropertiesException("ERROR: propertiesFile is null! Stopped.");				
			}	
		    OutputStream os = null;
		    try {
				os = new FileOutputStream(this.propertiesFile);
				this.properties.store(os, "Last update at " + new Date());
			    os.flush();
			} catch (FileNotFoundException e) {
				throw new MyPropertiesException(e);
			} catch (IOException e) {
				throw new MyPropertiesException(e);
			} finally {
				if (os != null) {
					try {
						os.close();
					} catch (IOException e) {
						// Ignore
					}
				}
			}
	  }	  

}
