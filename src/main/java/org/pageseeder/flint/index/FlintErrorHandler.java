/*
 * Copyright 2015 Allette Systems (Australia)
 * http://www.allette.com.au
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pageseeder.flint.index;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * A ruthless error handler.
 *
 * <p>Any error or warning will throw an exception.
 *
 * @author Jean-Baptiste Reure
 * @version 26 February 2010
 */
public class FlintErrorHandler implements ErrorHandler {

  // TODO: make a little more lenient!

  @Override
  public void error(SAXParseException exc) throws SAXException {
    throw exc;
  }

  @Override
  public void fatalError(SAXParseException exc) throws SAXException {
    throw exc;
  }

  @Override
  public void warning(SAXParseException exc) throws SAXException {
    throw exc;
  }

}
