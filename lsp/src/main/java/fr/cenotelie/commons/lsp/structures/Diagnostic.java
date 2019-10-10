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

package fr.cenotelie.commons.lsp.structures;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Represents a diagnostic, such as a compiler error or warning. Diagnostic objects are only valid in the scope of a resource.
 *
 * @author Laurent Wouters
 */
public class Diagnostic implements Serializable {
    /**
     * The range at which the message applies.
     */
    private final Range range;

    /**
     * The diagnostic's severity. Can be omitted. If omitted it is up to the
     * client to interpret diagnostics as error, warning, info or hint.
     */
    private final int severity;

    /**
     * The diagnostic's code. Can be omitted.
     */
    private final Object code;

    /**
     * A human-readable string describing the source of this
     * diagnostic, e.g. 'typescript' or 'super lint'.
     */
    private final String source;

    /**
     * The diagnostic's message.
     */
    private final String message;

    /**
     * Gets the range at which the message applies.
     *
     * @return The range at which the message applies.
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the diagnostic's severity
     *
     * @return The diagnostic's severity
     */
    public int getSeverity() {
        return severity;
    }

    /**
     * Gets the diagnostic's code. Can be omitted.
     *
     * @return The diagnostic's code. Can be omitted.
     */
    public Object getCode() {
        return code;
    }

    /**
     * Gets a human-readable string describing the source of this diagnostic
     *
     * @return A human-readable string describing the source of this diagnostic
     */
    public String getSource() {
        return source;
    }

    /**
     * Gets the diagnostic's message.
     *
     * @return The diagnostic's message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Initializes this structure
     *
     * @param range    The range at which the message applies.
     * @param severity The diagnostic's severity
     * @param code     The diagnostic's code. Can be omitted.
     * @param source   A human-readable string describing the source of this diagnostic
     * @param message  The diagnostic's message.
     */
    public Diagnostic(Range range, int severity, Object code, String source, String message) {
        this.range = range;
        this.severity = severity;
        this.code = code;
        this.source = source;
        this.message = message;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public Diagnostic(ASTNode definition) {
        Range range = null;
        int severity = DiagnosticSeverity.HINT;
        Object code = null;
        String source = null;
        String message = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "range": {
                    range = new Range(nodeValue);
                    break;
                }
                case "severity": {
                    severity = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "code": {
                    switch (nodeValue.getSymbol().getID()) {
                        case JsonLexer.ID.LITERAL_INTEGER:
                            code = Integer.parseInt(definition.getValue());
                            break;
                        case JsonLexer.ID.LITERAL_DECIMAL:
                            code = Double.parseDouble(definition.getValue());
                            break;
                        case JsonLexer.ID.LITERAL_DOUBLE:
                            code = Double.parseDouble(definition.getValue());
                            break;
                        case JsonLexer.ID.LITERAL_STRING:
                            String value = TextUtils.unescape(definition.getValue());
                            code = value.substring(1, value.length() - 1);
                            break;
                    }
                    break;
                }
                case "source": {
                    source = TextUtils.unescape(nodeValue.getValue());
                    source = source.substring(1, source.length() - 1);
                    break;
                }
                case "message": {
                    message = TextUtils.unescape(nodeValue.getValue());
                    message = message.substring(1, message.length() - 1);
                    break;
                }
            }
        }
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
        this.severity = severity;
        this.code = code;
        this.source = source;
        this.message = message;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"range\": ");
        builder.append(range.serializedJSON());
        builder.append(", \"severity\": ");
        builder.append(severity);
        if (code != null) {
            builder.append(", \"code\": ");
            Json.serialize(builder, code);
        }
        if (source != null) {
            builder.append(", \"source\": \"");
            builder.append(TextUtils.escapeStringJSON(source));
            builder.append("\"");
        }
        builder.append(", \"message\": \"");
        builder.append(TextUtils.escapeStringJSON(message));
        builder.append("\"}");
        return builder.toString();
    }
}
