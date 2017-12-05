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
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Represents a completion item to be displayed on the client
 *
 * @author Laurent Wouters
 */
public class CompletionItem implements Serializable {
    /**
     * The label of this completion item.
     * By default also the text that is inserted when selecting this completion.
     */
    private final String label;
    /**
     * The kind of this completion item.
     * Based of the kind an icon is chosen by the editor.
     */
    private final int kind;
    /**
     * A human-readable string with additional information about this item, like type or symbol information.
     */
    private String detail;
    /**
     * A human-readable string that represents a doc-comment.
     */
    private Object documentation;
    /**
     * A string that should be used when comparing this item with other items.
     * When `falsy` the label is used.
     */
    private String sortText;
    /**
     * A string that should be used when filtering a set of completion items.
     * When `falsy` the label is used.
     */
    private String filterText;
    /**
     * A string that should be inserted a document when selecting this completion.
     * When `falsy` the label is used.
     */
    private String insertText;
    /**
     * The format of the insert text.
     * The format applies to both the `insertText` property and the `newText` property of a provided `textEdit`.
     */
    private int insertTextFormat;
    /**
     * An edit which is applied to a document when selecting this completion.
     * When an edit is provided the value of `insertText` is ignored.
     * Note: The range of the edit must be a single line range and it must contain the position at which completion has been requested.
     */
    private TextEdit textEdit;
    /**
     * An optional array of additional text edits that are applied when selecting this completion.
     * Edits must not overlap with the main edit nor with themselves.
     */
    private TextEdit[] additionalTextEdits;
    /**
     * An optional command that is executed *after* inserting this completion.
     * Note that additional modifications to the current document should be described with the additionalTextEdits.
     */
    private Command command;
    /**
     * An data entry field that is preserved on a completion item between a completion and a completion resolve request.
     */
    private Object data;

    /**
     * Gets the label of this completion item
     *
     * @return The label of this completion item
     */
    public String getLabel() {
        return label;
    }

    /**
     * Gets the kind of this completion item
     *
     * @return The kind of this completion item
     */
    public int getKind() {
        return kind;
    }

    /**
     * Gets the human-readable string with additional information about this item, like type or symbol information
     *
     * @return The human-readable string with additional information about this item, like type or symbol information
     */
    public String getDetail() {
        return detail;
    }

    /**
     * Sets the human-readable string with additional information about this item, like type or symbol information
     *
     * @param detail the human-readable string with additional information about this item, like type or symbol information
     */
    public void setDetail(String detail) {
        this.detail = detail;
    }

    /**
     * Gets the human-readable string that represents a doc-comment
     *
     * @return The human-readable string that represents a doc-comment
     */
    public Object getDocumentation() {
        return documentation;
    }

    /**
     * Sets the human-readable string that represents a doc-comment
     *
     * @param documentation The human-readable string that represents a doc-comment
     */
    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    /**
     * Sets the human-readable string that represents a doc-comment
     *
     * @param documentation The human-readable string that represents a doc-comment
     */
    public void setDocumentation(MarkupContent documentation) {
        this.documentation = documentation;
    }

    /**
     * Gets the string that should be used when comparing this item with other items
     *
     * @return The string that should be used when comparing this item with other items
     */
    public String getSortText() {
        return sortText;
    }

    /**
     * Sets the string that should be used when comparing this item with other items
     *
     * @param sortText The string that should be used when comparing this item with other items
     */
    public void setSortText(String sortText) {
        this.sortText = sortText;
    }

    /**
     * Gets the string that should be used when filtering a set of completion items
     *
     * @return The string that should be used when filtering a set of completion items
     */
    public String getFilterText() {
        return filterText;
    }

    /**
     * Sets the string that should be used when filtering a set of completion items
     *
     * @param filterText The string that should be used when filtering a set of completion items
     */
    public void setFilterText(String filterText) {
        this.filterText = filterText;
    }

    /**
     * Gets the string that should be inserted a document when selecting this completion
     *
     * @return The string that should be inserted a document when selecting this completion
     */
    public String getInsertText() {
        return insertText;
    }

    /**
     * Sets the string that should be inserted a document when selecting this completion
     *
     * @param insertText The string that should be inserted a document when selecting this completion
     */
    public void setInsertText(String insertText) {
        this.insertText = insertText;
    }

    /**
     * Gets the format of the insert text
     *
     * @return The format of the insert text
     */
    public int getInsertTextFormat() {
        return insertTextFormat;
    }

    /**
     * Sets the format of the insert text
     *
     * @param insertTextFormat The format of the insert text
     */
    public void setInsertTextFormat(int insertTextFormat) {
        this.insertTextFormat = insertTextFormat;
    }

    /**
     * Gets the edit which is applied to a document when selecting this completion
     *
     * @return The edit which is applied to a document when selecting this completion
     */
    public TextEdit getTextEdit() {
        return textEdit;
    }

    /**
     * Sets the edit which is applied to a document when selecting this completion
     *
     * @param textEdit The edit which is applied to a document when selecting this completion
     */
    public void setTextEdit(TextEdit textEdit) {
        this.textEdit = textEdit;
    }

    /**
     * Gets the optional array of additional text edits that are applied when selecting this completion
     *
     * @return The optional array of additional text edits that are applied when selecting this completion
     */
    public TextEdit[] getAdditionalTextEdits() {
        return additionalTextEdits;
    }

    /**
     * Sets the optional array of additional text edits that are applied when selecting this completion
     *
     * @param additionalTextEdits The optional array of additional text edits that are applied when selecting this completion
     */
    public void setAdditionalTextEdits(TextEdit[] additionalTextEdits) {
        this.additionalTextEdits = additionalTextEdits;
    }

    /**
     * Gets the optional command that is executed *after* inserting this completion
     *
     * @return The optional command that is executed *after* inserting this completion
     */
    public Command getCommand() {
        return command;
    }

    /**
     * Sets the optional command that is executed *after* inserting this completion
     *
     * @param command The optional command that is executed *after* inserting this completion
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * Gets the data entry field that is preserved on a completion item between a completion and a completion resolve request
     *
     * @return The data entry field that is preserved on a completion item between a completion and a completion resolve request
     */
    public Object getData() {
        return data;
    }

    /**
     * Sets the data entry field that is preserved on a completion item between a completion and a completion resolve request
     *
     * @param data The data entry field that is preserved on a completion item between a completion and a completion resolve request
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * Initializes this structure
     *
     * @param label The label of this completion item
     * @param kind  The kind of this completion item
     */
    public CompletionItem(String label, int kind) {
        this.label = label;
        this.kind = kind;
        this.insertTextFormat = InsertTextFormat.PLAIN_TEXT;
    }

    /**
     * Initializes this structure
     *
     * @param label The label of this completion item
     */
    public CompletionItem(String label) {
        this(label, CompletionItemKind.TEXT);
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public CompletionItem(ASTNode definition, JsonDeserializer deserializer) {
        String label = "";
        int kind = CompletionItemKind.TEXT;
        String detail = null;
        Object documentation = null;
        String sortText = null;
        String filterText = null;
        String insertText = null;
        int insertTextFormat = InsertTextFormat.PLAIN_TEXT;
        TextEdit textEdit = null;
        TextEdit[] additionalTextEdits = null;
        Command command = null;
        Object data = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "label": {
                    label = TextUtils.unescape(nodeValue.getValue());
                    label = label.substring(1, label.length() - 1);
                    break;
                }
                case "kind": {
                    kind = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "detail": {
                    detail = TextUtils.unescape(nodeValue.getValue());
                    detail = detail.substring(1, detail.length() - 1);
                    break;
                }
                case "documentation": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.object)
                        documentation = new MarkupContent(nodeValue);
                    else {
                        String value = TextUtils.unescape(nodeValue.getValue());
                        documentation = value.substring(1, value.length() - 1);
                    }
                    break;
                }
                case "sortText": {
                    sortText = TextUtils.unescape(nodeValue.getValue());
                    sortText = sortText.substring(1, sortText.length() - 1);
                    break;
                }
                case "filterText": {
                    filterText = TextUtils.unescape(nodeValue.getValue());
                    filterText = filterText.substring(1, filterText.length() - 1);
                    break;
                }
                case "insertText": {
                    insertText = TextUtils.unescape(nodeValue.getValue());
                    insertText = insertText.substring(1, insertText.length() - 1);
                    break;
                }
                case "insertTextFormat": {
                    insertTextFormat = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "textEdit": {
                    textEdit = new TextEdit(nodeValue);
                    break;
                }
                case "additionalTextEdits": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                        additionalTextEdits = new TextEdit[nodeValue.getChildren().size()];
                        int index = 0;
                        for (ASTNode nodeItem : nodeValue.getChildren()) {
                            additionalTextEdits[index++] = new TextEdit(nodeItem);
                        }
                    }
                    break;
                }
                case "command": {
                    command = new Command(nodeValue, deserializer);
                    break;
                }
                case "data": {
                    data = deserializer.deserialize(nodeValue, this);
                    break;
                }
            }
        }
        this.label = label;
        this.kind = kind;
        this.detail = detail;
        this.documentation = documentation;
        this.sortText = sortText;
        this.filterText = filterText;
        this.insertText = insertText;
        this.insertTextFormat = insertTextFormat;
        this.textEdit = textEdit;
        this.additionalTextEdits = additionalTextEdits;
        this.command = command;
        this.data = data;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"label\": \"");
        builder.append(TextUtils.escapeStringJSON(label));
        builder.append("\"");
        if (kind > 0) {
            builder.append(", \"kind\": ");
            builder.append(Integer.toString(kind));
        }
        if (detail != null) {
            builder.append(", \"detail\": \"");
            builder.append(TextUtils.escapeStringJSON(detail));
            builder.append("\"");
        }
        if (documentation != null) {
            builder.append(", \"documentation\": ");
            Json.serialize(builder, documentation);
        }
        if (sortText != null) {
            builder.append(", \"sortText\": \"");
            builder.append(TextUtils.escapeStringJSON(sortText));
            builder.append("\"");
        }
        if (filterText != null) {
            builder.append(", \"filterText\": \"");
            builder.append(TextUtils.escapeStringJSON(filterText));
            builder.append("\"");
        }
        if (insertText != null) {
            builder.append(", \"insertText\": \"");
            builder.append(TextUtils.escapeStringJSON(insertText));
            builder.append("\"");
        }
        if (insertTextFormat > 0) {
            builder.append(", \"insertTextFormat\": ");
            builder.append(Integer.toString(insertTextFormat));
        }
        if (textEdit != null) {
            builder.append(", \"textEdit\": ");
            builder.append(textEdit.serializedJSON());
        }
        if (additionalTextEdits != null) {
            builder.append(", \"additionalTextEdits\": [");
            for (int i = 0; i != additionalTextEdits.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append(additionalTextEdits[i].serializedJSON());
            }
            builder.append("]");
        }
        if (command != null) {
            builder.append(", \"command\": ");
            builder.append(command.serializedJSON());
        }
        if (data != null) {
            builder.append(", \"data\": ");
            Json.serialize(builder, data);
        }
        builder.append("}");
        return builder.toString();
    }
}
