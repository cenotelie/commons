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
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

import java.util.Arrays;

/**
 * Document on-type formatting command registration options
 *
 * @author Laurent Wouters
 */
public class DocumentOnTypeFormattingRegistrationOptions implements Serializable {
    /**
     * A character on which formatting should be triggered, like '}'
     */
    private final String firstTriggerCharacter;
    /**
     * More trigger characters
     */
    private final String[] moreTriggerCharacter;

    /**
     * Gets the first character on which formatting should be triggered
     *
     * @return The first character on which formatting should be triggered
     */
    public String getFirstTriggerCharacter() {
        return firstTriggerCharacter;
    }

    /**
     * Gets more trigger characters
     *
     * @return More trigger characters
     */
    public String[] getMoreTriggerCharacter() {
        return moreTriggerCharacter;
    }

    /**
     * Initializes this structure
     *
     * @param triggerCharacter The trigger character
     */
    public DocumentOnTypeFormattingRegistrationOptions(String triggerCharacter) {
        this(triggerCharacter, null);
    }

    /**
     * Initializes this structure
     *
     * @param triggerCharacters The trigger characters
     */
    public DocumentOnTypeFormattingRegistrationOptions(String... triggerCharacters) {
        if (triggerCharacters.length == 0) {
            this.firstTriggerCharacter = "";
            this.moreTriggerCharacter = null;
        } else {
            this.firstTriggerCharacter = triggerCharacters[0];
            this.moreTriggerCharacter = Arrays.copyOfRange(triggerCharacters, 1, triggerCharacters.length);
        }
    }

    /**
     * Initializes this structure
     *
     * @param firstTriggerCharacter A character on which formatting should be triggered, like '}'
     * @param moreTriggerCharacter  More trigger characters
     */
    public DocumentOnTypeFormattingRegistrationOptions(String firstTriggerCharacter, String[] moreTriggerCharacter) {
        this.firstTriggerCharacter = firstTriggerCharacter;
        this.moreTriggerCharacter = moreTriggerCharacter;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DocumentOnTypeFormattingRegistrationOptions(ASTNode definition) {
        String firstTriggerCharacter = "";
        String[] moreTriggerCharacter = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "firstTriggerCharacter": {
                    firstTriggerCharacter = TextUtils.unescape(nodeValue.getValue());
                    firstTriggerCharacter = firstTriggerCharacter.substring(1, firstTriggerCharacter.length() - 1);
                    break;
                }
                case "moreTriggerCharacter": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                        moreTriggerCharacter = new String[nodeValue.getChildren().size()];
                        int index = 0;
                        for (ASTNode nodeItem : nodeValue.getChildren()) {
                            String value = TextUtils.unescape(nodeItem.getValue());
                            value = value.substring(1, value.length() - 1);
                            moreTriggerCharacter[index++] = value;
                        }
                    }
                    break;
                }
            }
        }
        this.firstTriggerCharacter = firstTriggerCharacter;
        this.moreTriggerCharacter = moreTriggerCharacter;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"firstTriggerCharacter\": \"");
        builder.append(TextUtils.escapeStringJSON(firstTriggerCharacter));
        builder.append("\"");
        if (moreTriggerCharacter != null) {
            builder.append(", \"moreTriggerCharacter\": [");
            for (int i = 0; i != moreTriggerCharacter.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append("\"");
                builder.append(TextUtils.escapeStringJSON(moreTriggerCharacter[i]));
                builder.append("\"");
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }
}
