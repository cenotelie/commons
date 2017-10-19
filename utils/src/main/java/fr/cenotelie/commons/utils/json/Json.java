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

package fr.cenotelie.commons.utils.json;

import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.logging.Logger;
import fr.cenotelie.hime.redist.ASTNode;
import fr.cenotelie.hime.redist.ParseError;
import fr.cenotelie.hime.redist.ParseResult;
import fr.cenotelie.hime.redist.TextContext;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.util.Collection;

/**
 * Json utility APIs
 *
 * @author Laurent Wouters
 */
public class Json {
    /**
     * Parses the JSON content
     *
     * @param logger  The logger to use
     * @param content The content to parse
     * @return The AST root node, or null of the parsing failed
     */
    public static ASTNode parse(Logger logger, String content) {
        return parse(logger, new StringReader(content));
    }

    /**
     * Parses the JSON content
     *
     * @param logger The logger to use
     * @param reader The reader with the content to parse
     * @return The AST root node, or null of the parsing failed
     */
    public static ASTNode parse(Logger logger, Reader reader) {
        ParseResult result = doParse(logger, reader);
        if (result == null)
            return null;
        if (!result.getErrors().isEmpty()) {
            for (ParseError error : result.getErrors())
                logger.error(error);
            return null;
        }
        return result.getRoot();
    }

    /**
     * Parses the JSON content
     *
     * @param logger The logger to use
     * @param reader The reader with the content to parse
     * @return The parse result
     */
    public static ParseResult doParse(Logger logger, Reader reader) {
        ParseResult result;
        try {
            String content = IOUtils.read(reader);
            JsonLexer lexer = new JsonLexer(content);
            JsonParser parser = new JsonParser(lexer);
            lexer.setRecoveryDistance(0);
            parser.setModeRecoverErrors(false);
            result = parser.parse();
        } catch (IOException ex) {
            logger.error(ex);
            return null;
        }
        for (ParseError error : result.getErrors()) {
            logger.error(error);
            TextContext context = result.getInput().getContext(error.getPosition(), error.getLength());
            logger.error(context.getContent());
            logger.error(context.getPointer());
        }
        return result;
    }

    /**
     * Gets the JSON serialization of the specified object
     *
     * @param object The object to serialize
     * @return The serialized object
     */
    public static String serialize(Object object) {
        if (object == null) {
            return "null";
        } else if (object instanceof Integer) {
            return Integer.toString((Integer) object);
        } else if (object instanceof Long) {
            return Long.toString((Long) object);
        } else if (object instanceof Float) {
            return Float.toString((Float) object);
        } else if (object instanceof Double) {
            return Double.toString((Double) object);
        } else if (object instanceof Boolean) {
            return Boolean.toString((Boolean) object);
        } else if (object instanceof Serializable) {
            return ((Serializable) object).serializedJSON();
        } else if (object.getClass().isArray()) {
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            int length = Array.getLength(object);
            for (int i = 0; i != length; i++) {
                if (i != 0)
                    builder.append(", ");
                serialize(builder, Array.get(object, i));
            }
            builder.append("]");
            return builder.toString();
        } else if (object instanceof Collection) {
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            boolean first = true;
            for (Object value : ((Collection<?>) object)) {
                if (!first)
                    builder.append(", ");
                first = false;
                serialize(builder, value);
            }
            builder.append("]");
            return builder.toString();
        } else {
            return "\"" + TextUtils.escapeStringJSON(object.toString()) + "\"";
        }
    }

    /**
     * Builds the JSON serialization of the specified object
     *
     * @param builder The string build to output to
     * @param object  The object to serialize
     */
    public static void serialize(StringBuilder builder, Object object) {
        if (object == null) {
            builder.append("null");
        } else if (object instanceof Integer) {
            builder.append(Integer.toString((Integer) object));
        } else if (object instanceof Long) {
            builder.append(Long.toString((Long) object));
        } else if (object instanceof Float) {
            builder.append(Float.toString((Float) object));
        } else if (object instanceof Double) {
            builder.append(Double.toString((Double) object));
        } else if (object instanceof Boolean) {
            builder.append(Boolean.toString((Boolean) object));
        } else if (object instanceof Serializable) {
            builder.append(((Serializable) object).serializedJSON());
        } else if (object.getClass().isArray()) {
            builder.append("[");
            int length = Array.getLength(object);
            for (int i = 0; i != length; i++) {
                if (i != 0)
                    builder.append(", ");
                serialize(builder, Array.get(object, i));
            }
            builder.append("]");
        } else if (object instanceof Collection) {
            builder.append("[");
            boolean first = true;
            for (Object value : ((Collection<?>) object)) {
                if (!first)
                    builder.append(", ");
                first = false;
                serialize(builder, value);
            }
            builder.append("]");
        } else {
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(object.toString()));
            builder.append("\"");
        }
    }
}
