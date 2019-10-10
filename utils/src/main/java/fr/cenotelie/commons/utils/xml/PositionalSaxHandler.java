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

import fr.cenotelie.hime.redist.TextPosition;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Stack;

/**
 * XML SAX parsing handler for the support of the position of XML elements
 *
 * @author Laurent Wouters
 */
class PositionalSaxHandler extends DefaultHandler implements LexicalHandler {
    /**
     * The document to build
     */
    private final Document document;
    /**
     * The stack of element nodes
     */
    private final Stack<Node> stack;
    /**
     * The buffer of found text
     */
    private final StringBuilder textBuffer;
    /**
     * The locator to use
     */
    private Locator locator;
    /**
     * The last known location
     */
    private TextPosition lastLocation;

    /**
     * Initializes this handler
     *
     * @param document The document to build
     */
    public PositionalSaxHandler(Document document) {
        this.document = document;
        this.stack = new Stack<>();
        this.stack.push(document);
        this.textBuffer = new StringBuilder();
        this.lastLocation = new TextPosition(0, 0);
    }

    @Override
    public void setDocumentLocator(final Locator locator) {
        this.locator = locator;
    }

    @Override
    public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) throws SAXException {
        appendText();
        Element element = document.createElement(qName);
        for (int i = 0; i < attributes.getLength(); i++) {
            element.setAttribute(attributes.getQName(i), attributes.getValue(i));
        }
        element.setUserData(Xml.KEY_POSITION_OPENING_START, lastLocation, null);
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
        element.setUserData(Xml.KEY_POSITION_OPENING_END, lastLocation, null);
        stack.push(element);
    }

    @Override
    public void endElement(final String uri, final String localName, final String qName) {
        appendText();
        Node closed = stack.pop();
        Node parent = stack.peek();
        parent.appendChild(closed);
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void characters(final char[] ch, final int start, final int length) throws SAXException {
        textBuffer.append(ch, start, length);
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
        super.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
        super.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
        super.skippedEntity(name);
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void endDTD() throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void startEntity(String name) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void endEntity(String name) throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void startCDATA() throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    @Override
    public void endCDATA() throws SAXException {
        lastLocation = new TextPosition(locator.getLineNumber(), locator.getColumnNumber());
    }

    /**
     * Appends text found so far to the top stack element
     */
    private void appendText() {
        if (textBuffer.length() > 0) {
            Node element = stack.peek();
            Node textNode = document.createTextNode(textBuffer.toString());
            element.appendChild(textNode);
            textBuffer.delete(0, textBuffer.length());
        }
    }
}
