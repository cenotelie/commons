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

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Represents a canonicalizer for RDF/XML inputs
 *
 * @author Laurent Wouters
 */
public class XmlCanonicalizer {
    /**
     * The output string writer
     */
    protected StringWriter output;
    /**
     * Flags whether the current document is XML 1.1
     */
    protected boolean xmlVersion11;

    /**
     * Canonicalizes the specified list of nodes
     *
     * @param nodes The XML nodes to canonicalize
     * @return The String representation of the canonical XML
     */
    public String canonicalize(NodeList nodes) {
        output = new StringWriter();
        onNodes(nodes);
        return output.toString();
    }

    /**
     * Canonicalizes the specified list of nodes
     *
     * @param nodes A list of nodes
     */
    private void onNodes(NodeList nodes) {
        for (int i = 0; i != nodes.getLength(); i++)
            onOne(nodes.item(i));
    }

    /**
     * Canonicalizes the specified node
     *
     * @param node A node
     */
    private void onOne(Node node) {
        switch (node.getNodeType()) {
            case Node.ELEMENT_NODE:
                onElementStart(node);
                onNodes(node.getChildNodes());
                onElementEnd(node);
                break;
            case Node.TEXT_NODE:
                onText(node);
                break;
        }
    }

    /**
     * Writes the canonical element start markup for the specified node
     *
     * @param node An element node
     */
    private void onElementStart(Node node) {
        output.write('<');
        output.write(node.getNodeName());

        List<Node> attributes = new ArrayList<>(node.getAttributes().getLength());
        for (int i = 0; i != node.getAttributes().getLength(); i++) {
            attributes.add(node.getAttributes().item(i));
        }
        attributes.sort(Comparator.comparing(Node::getNodeName));
        for (Node attribute : attributes)
            onAttribute(attribute);
        output.write('>');
    }

    /**
     * Writes the canonical representation of the specified attribute
     *
     * @param node An attribute node
     */
    private void onAttribute(Node node) {
        output.write(" ");
        output.write(node.getNodeName());
        output.write("=\"");
        normalizeAndPrint(node.getNodeValue(), true);
        output.write('"');
    }

    /**
     * Writes the canonical element end markup for the specified node
     *
     * @param node An element node
     */
    private void onElementEnd(Node node) {
        output.write("</");
        output.write(node.getNodeName());
        output.write('>');
    }

    /**
     * Writes the canonical representation of the specified text node
     *
     * @param node A text node
     */
    private void onText(Node node) {
        normalizeAndPrint(node.getNodeValue(), false);
    }

    /**
     * Normalizes and prints the specified string
     *
     * @param value            The string to normalize and print
     * @param isAttributeValue Whether this is the value of an attribute
     */
    private void normalizeAndPrint(String value, boolean isAttributeValue) {
        int len = (value != null) ? value.length() : 0;
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            normalizeAndPrint(c, isAttributeValue);
        }
    }

    /**
     * Normalizes and prints the specified character
     *
     * @param character        A character
     * @param isAttributeValue Whether this is the value of an attribute
     */
    private void normalizeAndPrint(char character, boolean isAttributeValue) {
        switch (character) {
            case '<': {
                output.write("&lt;");
                break;
            }
            case '>': {
                output.write("&gt;");
                break;
            }
            case '&': {
                output.write("&amp;");
                break;
            }
            case '"': {
                // A '"' that appears in character data
                // does not need to be escaped.
                if (isAttributeValue) {
                    output.write("&quot;");
                } else {
                    output.write("\"");
                }
                break;
            }
            case '\r': {
                // If CR is part of the document's content, it
                // must not be printed as a literal otherwise
                // it would be normalized to LF when the document
                // is reparsed.
                output.write("&#xD;");
                break;
            }
            case '\n': {
                output.write("&#xA;");
                break;
            }
            default: {
                // In XML 1.1, control chars in the ranges [#x1-#x1F, #x7F-#x9F] must be escaped.
                //
                // Escape space characters that would be normalized to #x20 in attribute values
                // when the document is reparsed.
                //
                // Escape NEL (0x85) and LSEP (0x2028) that appear in content
                // if the document is XML 1.1, since they would be normalized to LF
                // when the document is reparsed.
                if (xmlVersion11 && ((character >= 0x01 && character <= 0x1F && character != 0x09)
                        || (character >= 0x7F && character <= 0x9F) || character == 0x2028)
                        || isAttributeValue && character == 0x09) {
                    output.write("&#x");
                    output.write(Integer.toHexString(character).toUpperCase());
                    output.write(";");
                } else {
                    output.write(character);
                }
            }
        }
    }
}
