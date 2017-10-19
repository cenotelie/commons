/*******************************************************************************
 * Copyright (c) 2017 Association Cénotélie (cenotelie.fr)
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package fr.cenotelie.commons.utils.xml;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.Reader;

/**
 * Utility APIs for XML
 *
 * @author Laurent Wouters
 */
public class Xml {
    /**
     * The key for the position of the opening tag of an element
     */
    public static final String KEY_POSITION_OPENING_START = PositionalSaxHandler.class.getCanonicalName() + ".PositionOpeningStart";
    /**
     * The key for the position of the end of the opening tag of an element
     */
    public static final String KEY_POSITION_OPENING_END = PositionalSaxHandler.class.getCanonicalName() + ".PositionOpeningEnd";

    /**
     * Parses the input reader as an XML document
     *
     * @param reader The reader to use as input
     * @return The produced XML document
     * @throws Exception When a parsing error occurred
     */
    public static Document parse(Reader reader) throws Exception {
        DocumentBuilderFactory xmlFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = xmlFactory.newDocumentBuilder();
        Document xmlDocument = documentBuilder.newDocument();
        SAXParserFactory parserFactory = SAXParserFactory.newInstance();
        parserFactory.setValidating(true);
        SAXParser parser = parserFactory.newSAXParser();
        PositionalSaxHandler handler = new PositionalSaxHandler(xmlDocument);
        parser.getXMLReader().setProperty("http://xml.org/sax/properties/lexical-handler", handler);
        parser.parse(new InputSource(reader), handler);
        return xmlDocument;
    }
}
