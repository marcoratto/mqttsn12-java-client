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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Costituisce la superclasse di tutte le Exception dell'Infrastruttura.
 * <BR>Non ne &egrave; previsto l'utilizzo diretto (istanziazione e/o lancio), ma pu&ograve; essere utilizzata nella gestione del <i>trapping</i>, in modo da poter trattare facilmente tutte le Exception di Infrastruttura.
 * @author Marco Ratto
 */
public class MyPropertiesException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3348677367947352434L;
	
	private final static Logger logger = LoggerFactory.getLogger(MyPropertiesException.class);
	
  public MyPropertiesException(String s) {
    super(s);
    logger.error(s);
  }

  public MyPropertiesException(String s, Exception e) {
    super(s, e);
    logger.error(s, e);
  }

  public MyPropertiesException(Exception e) {
    super(e);
    logger.error(e.getMessage(), e);
  }

  public MyPropertiesException() {
    super();
    logger.error("");
  }
}
