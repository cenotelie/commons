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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.collections.AdaptingIterator;
import fr.cenotelie.commons.utils.collections.Couple;
import fr.cenotelie.commons.utils.http.URIUtils;
import fr.cenotelie.hime.redist.TextPosition;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.*;

/**
 * Represents an XML element with its contextual information
 *
 * @author Laurent Wouters
 */
public class XmlElement implements Iterable<XmlElement> {
    /**
     * The parent contextual element
     */
    private XmlElement parent;
    /**
     * The represented XML element node
     */
    private final Element node;
    /**
     * The node complete IRI
     */
    private String nodeIRI;
    /**
     * The current list of attributes
     */
    private Map<String, Node> attributes;
    /**
     * IRI of the document's parent
     */
    private final String resource;
    /**
     * Maps of the current namespaces
     */
    private final Map<String, String> namespaces;
    /**
     * The current XML namespace
     */
    private String currentNamespace;
    /**
     * The current base URI
     */
    private String baseURI;
    /**
     * The current language
     */
    private String language;

    /**
     * Gets the IRI of this node
     *
     * @return The IRI of this node
     */
    public String getNodeIRI() {
        return nodeIRI;
    }

    /**
     * Gets the local name of this node
     *
     * @return The local name of this node
     */
    public String getNodeName() {
        return node.getLocalName();
    }

    /**
     * Gets the current language
     *
     * @return The current language
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Gets whether this node is empty, i.e. it has no children at all
     *
     * @return <code>true</code> if this node is empty
     */
    public boolean isEmpty() {
        for (int i = 0; i != node.getChildNodes().getLength(); i++) {
            int type = node.getChildNodes().item(i).getNodeType();
            switch (type) {
                case Node.ELEMENT_NODE:
                case Node.TEXT_NODE:
                    return false;
            }
        }
        return true;
    }

    /**
     * Gets the index of this node relative to its siblings with the same IRI
     *
     * @return The index of this node
     */
    public int getIndex() {
        Iterator<XmlElement> siblings = parent.getChildren();
        int index = 1;
        while (siblings.hasNext()) {
            XmlElement potential = siblings.next();
            if (potential.node == this.node)
                return index;
            if (this.nodeIRI.equals(potential.nodeIRI))
                index++;
        }
        return 0;
    }

    /**
     * Gets the inner text content
     *
     * @return The inner text content
     */
    public String getContent() {
        return node.getChildNodes().item(0).getNodeValue();
    }

    /**
     * Gets the XML literal representation of the content of this node
     *
     * @return The XML literal representation of the content of this node
     */
    public String getXMLLiteral() {
        XmlCanonicalizer canonicalizer = new XmlCanonicalizer();
        return canonicalizer.canonicalize(node.getChildNodes());
    }

    /**
     * Gets the text position for the start of the opening tag of this element
     *
     * @return The text position
     */
    public TextPosition getPositionOpeningStart() {
        return (TextPosition) node.getUserData(Xml.KEY_POSITION_OPENING_START);
    }

    /**
     * Gets the text position for the end of the opening tag of this element
     *
     * @return The text position
     */
    public TextPosition getPositionOpeningEnd() {
        return (TextPosition) node.getUserData(Xml.KEY_POSITION_OPENING_END);
    }

    /**
     * Initializes this root node
     *
     * @param node     The represented XML node
     * @param resource The resource's URI
     */
    public XmlElement(Element node, String resource) {
        this.node = node;
        this.resource = resource;
        this.namespaces = new HashMap<>();
        init();
    }

    /**
     * Initializes this node with a parent node
     *
     * @param parent The parent contextual node
     * @param node   The represented XML node
     */
    public XmlElement(XmlElement parent, Element node) {
        this.parent = parent;
        this.node = node;
        this.resource = parent.resource;
        this.namespaces = new HashMap<>();
        this.currentNamespace = parent.currentNamespace;
        this.baseURI = parent.baseURI;
        this.language = parent.language;
        init();
    }

    /**
     * Initializes the contextual data
     */
    private void init() {
        List<Node> userAttributes = new ArrayList<>();
        for (int i = 0; i != node.getAttributes().getLength(); i++) {
            org.w3c.dom.Node attribute = node.getAttributes().item(i);
            String name = attribute.getNodeName();
            if ("xml:lang".equals(name)) {
                language = attribute.getNodeValue();
            } else if ("xml:base".equals(name)) {
                baseURI = sanitizeBaseURI(attribute.getNodeValue());
            } else if ("xmlns".equals(name)) {
                currentNamespace = attribute.getNodeValue();
            } else if (name.startsWith("xmlns:")) {
                namespaces.put(name.substring(6), attribute.getNodeValue());
            } else if (!name.startsWith("xml")) {
                userAttributes.add(attribute);
            }
        }

        nodeIRI = resolveLocalName(node.getNodeName());

        attributes = new HashMap<>();
        for (org.w3c.dom.Node attribute : userAttributes) {
            String name = attribute.getNodeName();
            name = resolveLocalName(name);
            attributes.put(name, attribute);
        }
    }

    /**
     * Sanitizes the specified base URI by removing the fragment component, if any
     *
     * @param value A base URI
     * @return The equivalent sanitized base URI
     */
    private String sanitizeBaseURI(String value) {
        String[] components = URIUtils.parse(value);
        return URIUtils.recompose(components[URIUtils.COMPONENT_SCHEME], components[URIUtils.COMPONENT_AUTHORITY], components[URIUtils.COMPONENT_PATH], components[URIUtils.COMPONENT_QUERY], null);
    }


    /**
     * Resolves the specified local name with the contextual elements of this node and its ancestors
     *
     * @param localName A local name
     * @return The resulting resolved URI
     */
    private String resolveLocalName(String localName) {
        XmlElement current = this;
        localName = TextUtils.unescape(localName);
        if (!localName.contains(":"))
            return currentNamespace + localName;
        while (current != null) {
            int index = 0;
            while (index != localName.length()) {
                if (localName.charAt(index) == ':') {
                    String prefix = localName.substring(0, index);
                    String uri = current.namespaces.get(prefix);
                    if (uri != null) {
                        String name = localName.substring(index + 1);
                        return URIUtils.resolveRelative(baseURI, TextUtils.unescape(uri + name));
                    }
                }
                index++;
            }
            current = current.parent;
        }
        throw new IllegalArgumentException("Failed to resolve local name " + localName);
    }

    /**
     * Resolves a possibly relative IRI against this element
     *
     * @param iri A possibly relative IRI
     * @return The resolved and normalized IRI
     */
    public String resolve(String iri) {
        return URIUtils.resolveRelative(baseURI != null ? baseURI : resource, TextUtils.unescape(iri));
    }

    /**
     * Gets the attributes with the specified name and removes it
     *
     * @param name An attributes's name
     * @return The corresponding attribute, or <code>null</code> if none is found
     */
    public String getAttribute(String name) {
        Node attribute = attributes.get(name);
        if (attribute != null)
            attributes.remove(name);
        return (attribute != null) ? attribute.getNodeValue() : null;
    }

    /**
     * Gets an iterator over the attributes of this element
     *
     * @return An iterator over the remaining attributes of this element
     */
    public Iterator<Couple<String, String>> getAttributes() {
        return new AdaptingIterator<>(
                attributes.keySet().iterator(),
                element -> new Couple<>(element, attributes.get(element).getNodeValue()));
    }

    /**
     * Gets an iterator over the Element children of the specified XML node
     *
     * @return An iterator over the Element children
     */
    public Iterator<XmlElement> getChildren() {
        return iterator();
    }

    /**
     * Gets an iterator over the Element children of the specified XML node
     *
     * @return An iterator over the Element children
     */
    @Override
    public Iterator<XmlElement> iterator() {
        final NodeList list = node.getChildNodes();
        return new Iterator<XmlElement>() {
            int index = getNext(0);

            private int getNext(int start) {
                for (int i = start; i != list.getLength(); i++)
                    if (list.item(i).getNodeType() == Node.ELEMENT_NODE)
                        return i;
                return list.getLength();
            }

            @Override
            public boolean hasNext() {
                return (index != list.getLength());
            }

            @Override
            public XmlElement next() {
                Element result = (Element) list.item(index);
                index = getNext(index + 1);
                return new XmlElement(XmlElement.this, result);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
