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

import io.github.marcoratto.mqttsn.MqttSnClient;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @author Marco Ratto
 *
 */
public class TestTopic extends TestCase {
	
      
	protected void setUp() {
		System.out.println(this.getName() + ".setUp()");	

	}

	protected void tearDown() {
		System.out.println(this.getName() + ".tearDown()");

	}

	public static void main (String[] args) {
		TestRunner.run(suite());
	}
	
	public static Test suite() {
		return new TestSuite(TestTopic.class);
	}	

	public void test1() {
		System.out.println(this.getClass().getName() + ".test1()");
		try {						
			String topic = "";
			String filter = "#";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test2() {
		System.out.println(this.getClass().getName() + ".test2()");
		try {						
			String topic = "";
			String filter = "+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test3() {
		System.out.println(this.getClass().getName() + ".test3()");
		try {						
			String topic = "";
			String filter = "";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test4() {
		System.out.println(this.getClass().getName() + ".test4()");
		try {						
			String topic = "home/";
			String filter = "";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test5() {
		System.out.println(this.getClass().getName() + ".test5()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "home/+/temperature";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test6() {
		System.out.println(this.getClass().getName() + ".test6()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "home/#";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test7() {
		System.out.println(this.getClass().getName() + ".test7()");
		try {						
			String topic = "home/livingroom/temperature/humidity";
			String filter = "home/+/temperature/#";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test8() {
		System.out.println(this.getClass().getName() + ".test8()");
		try {						
			String topic = "";
			String filter = "home/#";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test9() {
		System.out.println(this.getClass().getName() + ".test9()");
		try {						
			String topic = "home/livingroom/kitchen/temperature";
			String filter = "home/+/+/temperature";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test10() {
		System.out.println(this.getClass().getName() + ".test10()");
		try {						
			String topic = "home/livingroom/temperature/sensor/data";
			String filter = "home/+/temperature/#";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test11() {
		System.out.println(this.getClass().getName() + ".test11()");
		try {						
			String topic = "home/kitchen/temperature";
			String filter = "+/+/temperature";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test12() {
		System.out.println(this.getClass().getName() + ".test12()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "+/+/+/temperature";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test13() {
		System.out.println(this.getClass().getName() + ".test13()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "+/livingroom/+/temperature/+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test14() {
		System.out.println(this.getClass().getName() + ".test14()");
		try {						
			String topic = "home/livingroom/kitchen/temperature";
			String filter = "home/+/+/temperature";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test15() {
		System.out.println(this.getClass().getName() + ".test15()");
		try {						
			String topic = "home/kitchen/bedroom/temperature";
			String filter = "home/+/+/temperature";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test16() {
		System.out.println(this.getClass().getName() + ".test16()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "home/+/+/temperature";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test17() {
		System.out.println(this.getClass().getName() + ".test17()");
		try {						
			String topic = "a/b/c";
			String filter = "+/+/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test18() {
		System.out.println(this.getClass().getName() + ".test18()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "+/+/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test19() {
		System.out.println(this.getClass().getName() + ".test19()");
		try {						
			String topic = "home/livingroom";
			String filter = "+/+/+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test20() {
		System.out.println(this.getClass().getName() + ".test20()");
		try {						
			String topic = "home/kitchen/temperature/humidity";
			String filter = "home/+/+/#";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test21() {
		System.out.println(this.getClass().getName() + ".test21()");
		try {						
			String topic = "home/room1/room2/sensor/data";
			String filter = "home/+/+/#";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test22() {
		System.out.println(this.getClass().getName() + ".test22()");
		try {						
			String topic = "home/livingroom";
			String filter = "home/+/+/#";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test23() {
		System.out.println(this.getClass().getName() + ".test23()");
		try {						
			String topic = "a/b";
			String filter = "+/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test24() {
		System.out.println(this.getClass().getName() + ".test24()");
		try {						
			String topic = "a/b/c";
			String filter = "+/+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test25() {
		System.out.println(this.getClass().getName() + ".test25()");
		try {						
			String topic = "a";
			String filter = "+/+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test26() {
		System.out.println(this.getClass().getName() + ".test26()");
		try {						
			String topic = "a/b/";
			String filter = "+/+/";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test27() {
		System.out.println(this.getClass().getName() + ".test27()");
		try {						
			String topic = "a/b/c";
			String filter = "+/+/";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test28() {
		System.out.println(this.getClass().getName() + ".test28()");
		try {						
			String topic = "a/";
			String filter = "+/+/";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test29() {
		System.out.println(this.getClass().getName() + ".test29()");
		try {						
			String topic = "a/b/c/d";
			String filter = "+/+/+/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test30() {
		System.out.println(this.getClass().getName() + ".test30()");
		try {						
			String topic = "home/livingroom/temperature/sensor";
			String filter = "+/+/+/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test31() {
		System.out.println(this.getClass().getName() + ".test31()");
		try {						
			String topic = "home/livingroom/temperature";
			String filter = "+/+/+/+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test32() {
		System.out.println(this.getClass().getName() + ".test32()");
		try {						
			String topic = "home/livingroom/kitchen/temperature/sensor";
			String filter = "+/livingroom/+/temperature/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test33() {
		System.out.println(this.getClass().getName() + ".test33()");
		try {						
			String topic = "office/livingroom/office/temperature/room1";
			String filter = "+/livingroom/+/temperature/+";
			boolean expected = true;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}
	public void test34() {
		System.out.println(this.getClass().getName() + ".test34()");
		try {						
			String topic = "home/kitchen/kitchen/temperature/sensor";
			String filter = "+/livingroom/+/temperature/+";
			boolean expected = false;
			boolean actual = MqttSnClient.isMatched(topic, filter);

			assertEquals(expected, actual);
			
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		} 		
	}

			
}
