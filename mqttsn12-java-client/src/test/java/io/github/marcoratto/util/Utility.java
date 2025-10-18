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

public class Utility {

	private static Utility instance = null;
	
	private Utility() {
	}

	  public void reset() {
		  instance = null;
	  }
	  
	  public static Utility getInstance() {
	    if (instance == null) {
	      synchronized(Utility.class) {
	        if (instance == null) {
	          instance = new Utility();
	        }
	      }
	    }
	    return instance;
	  }
	  
	public final void sleepMilliseconds(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			// Ignore
		}
	}
	
	public final void sleepSeconds(int sec) {
		this.sleepMilliseconds(sec * 1000);
	}

	

}
