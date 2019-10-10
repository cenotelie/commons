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

package fr.cenotelie.commons.jsonrpc;

import fr.cenotelie.commons.utils.api.Reply;
import fr.cenotelie.commons.utils.api.ReplyResult;
import fr.cenotelie.commons.utils.api.ReplyResultCollection;
import fr.cenotelie.commons.utils.api.ReplySuccess;
import fr.cenotelie.commons.utils.json.SerializedUnknown;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

/**
 * Test suite for the Json-Rcp protocol
 *
 * @author Laurent Wouters
 */
public class JsonRpcTest {

    @Test
    public void testPositionalParameters1() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"subtract".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                Object params = request.getParams();
                if (!(params instanceof Collection))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                Object[] values = ((Collection) params).toArray();
                if (values.length != 2 || !(values[0] instanceof Integer) || !(values[1] instanceof Integer))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                int result = (Integer) values[0] - (Integer) values[1];
                return new JsonRpcResponseResult<>(request.getIdentifier(), result);
            }
        };
        JsonRpcClient client = new TestClient(server);

        JsonRpcRequest request = new JsonRpcRequest("1", "subtract", new Object[]{42, 23});
        Reply reply = client.send(request);
        Assert.assertTrue(reply.isSuccess());
        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseResult);
        JsonRpcResponseResult<Integer> result = (JsonRpcResponseResult<Integer>) response;
        Assert.assertEquals("1", response.getIdentifier());
        Assert.assertEquals((Integer) 19, result.getResult());
    }

    @Test
    public void testPositionalParameters2() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"subtract".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                Object params = request.getParams();
                if (!(params instanceof Collection))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                Object[] values = ((Collection) params).toArray();
                if (values.length != 2 || !(values[0] instanceof Integer) || !(values[1] instanceof Integer))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                int result = (Integer) values[0] - (Integer) values[1];
                return new JsonRpcResponseResult<>(request.getIdentifier(), result);
            }
        };
        JsonRpcClient client = new TestClient(server);

        JsonRpcRequest request = new JsonRpcRequest("1", "subtract", new Object[]{23, 42});
        Reply reply = client.send(request);
        Assert.assertTrue(reply.isSuccess());
        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseResult);
        JsonRpcResponseResult<Integer> result = (JsonRpcResponseResult<Integer>) response;
        Assert.assertEquals("1", response.getIdentifier());
        Assert.assertEquals((Integer) (-19), result.getResult());
    }

    @Test
    public void testNamedParameters() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"subtract".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                Object params = request.getParams();
                if (!(params instanceof SerializedUnknown))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                Object subtrahend = ((SerializedUnknown) params).getValueFor("subtrahend");
                Object minuend = ((SerializedUnknown) params).getValueFor("minuend");
                if (!(subtrahend instanceof Integer)
                        || !(minuend instanceof Integer))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                int result = ((Integer) minuend) - (Integer) subtrahend;
                return new JsonRpcResponseResult<>(request.getIdentifier(), result);
            }
        };
        JsonRpcClient client = new TestClient(server);

        SerializedUnknown parameters = new SerializedUnknown();
        parameters.addProperty("subtrahend", 23);
        parameters.addProperty("minuend", 42);
        JsonRpcRequest request = new JsonRpcRequest("1", "subtract", parameters);
        Reply reply = client.send(request);
        Assert.assertTrue(reply.isSuccess());
        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseResult);
        JsonRpcResponseResult<Integer> result = (JsonRpcResponseResult<Integer>) response;
        Assert.assertEquals("1", response.getIdentifier());
        Assert.assertEquals((Integer) 19, result.getResult());
    }

    @Test
    public void testSimpleNotification1() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                Assert.assertTrue(request.isNotification());
                if (!"update".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                Object params = request.getParams();
                if (!(params instanceof Collection))
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                return null;
            }
        };
        JsonRpcClient client = new TestClient(server);

        JsonRpcRequest request = new JsonRpcRequest(null, "update", new Object[]{1, 2, 3, 4, 5});
        Assert.assertTrue(request.isNotification());
        Reply reply = client.send(request);
        Assert.assertEquals(ReplySuccess.instance(), reply);
    }

    @Test
    public void testSimpleNotification2() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                Assert.assertTrue(request.isNotification());
                if (!"foobar".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                Object params = request.getParams();
                if (params != null)
                    return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
                return null;
            }
        };
        JsonRpcClient client = new TestClient(server);

        JsonRpcRequest request = new JsonRpcRequest(null, "foobar", null);
        Assert.assertTrue(request.isNotification());
        Reply reply = client.send(request);
        Assert.assertEquals(ReplySuccess.instance(), reply);
    }

    @Test
    public void testMissingMethod() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        JsonRpcClient client = new TestClient(server);

        JsonRpcRequest request = new JsonRpcRequest("1", "foobar", null);
        Reply reply = client.send(request);
        Assert.assertTrue(reply.isSuccess());
        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseError);
        JsonRpcResponseError error = (JsonRpcResponseError) response;
        Assert.assertEquals("1", error.getIdentifier());
        Assert.assertEquals(-32601, error.getCode());
        Assert.assertEquals("Method not found", error.getMessage());
    }

    @Test
    public void testInvalidJson() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        TestClient client = new TestClient(server);

        String request = "{\"jsonrpc\": \"2.0\", \"method\": \"foobar, \"params\": \"bar\", \"baz]";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextSingle("foobar"));
        Assert.assertTrue(reply.isSuccess());

        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseError);
        JsonRpcResponseError error = (JsonRpcResponseError) response;
        Assert.assertNull(error.getIdentifier());
        Assert.assertEquals(-32700, error.getCode());
        Assert.assertEquals("Parse error", error.getMessage());
    }

    @Test
    public void testInvalidRequest() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        TestClient client = new TestClient(server);

        String request = "{\"jsonrpc\": \"2.0\", \"method\": 1, \"params\": \"bar\"}";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextSingle("1"));
        Assert.assertTrue(reply.isSuccess());

        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseError);
        JsonRpcResponseError error = (JsonRpcResponseError) response;
        Assert.assertNull(error.getIdentifier());
        Assert.assertEquals(-32600, error.getCode());
        Assert.assertEquals("Invalid Request", error.getMessage());
    }

    @Test
    public void testBatchInvalidJson() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        TestClient client = new TestClient(server);

        String request = "[\n" +
                "  {\"jsonrpc\": \"2.0\", \"method\": \"sum\", \"params\": [1,2,4], \"id\": \"1\"},\n" +
                "  {\"jsonrpc\": \"2.0\", \"method\"\n" +
                "]";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextSingle("sum"));
        Assert.assertTrue(reply.isSuccess());

        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseError);
        JsonRpcResponseError error = (JsonRpcResponseError) response;
        Assert.assertNull(error.getIdentifier());
        Assert.assertEquals(-32700, error.getCode());
        Assert.assertEquals("Parse error", error.getMessage());
    }

    @Test
    public void testBatchEmpty() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        TestClient client = new TestClient(server);

        String request = "[]";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextSingle(null));
        Assert.assertTrue(reply.isSuccess());

        JsonRpcResponse response = ((ReplyResult<JsonRpcResponse>) reply).getData();
        Assert.assertTrue(response instanceof JsonRpcResponseError);
        JsonRpcResponseError error = (JsonRpcResponseError) response;
        Assert.assertNull(error.getIdentifier());
        Assert.assertEquals(-32600, error.getCode());
        Assert.assertEquals("Invalid Request", error.getMessage());
    }

    @Test
    public void testBatchInvalid1() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        TestClient client = new TestClient(server);

        String request = "[1]";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextSingle(null));
        Assert.assertTrue(reply.isSuccess());

        Collection<JsonRpcResponse> responses = ((ReplyResultCollection<JsonRpcResponse>) reply).getData();
        Assert.assertEquals(1, responses.size());
        JsonRpcResponse response = responses.iterator().next();
        Assert.assertTrue(response instanceof JsonRpcResponseError);
        JsonRpcResponseError error = (JsonRpcResponseError) response;
        Assert.assertNull(error.getIdentifier());
        Assert.assertEquals(-32600, error.getCode());
        Assert.assertEquals("Invalid Request", error.getMessage());
    }

    @Test
    public void testBatchInvalid2() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (!"myMethod".equals(request.getMethod()))
                    return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
                return new JsonRpcResponseResult<>(request.getIdentifier(), "ok");
            }
        };
        TestClient client = new TestClient(server);

        String request = "[1,2,3]";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextSingle(null));
        Assert.assertTrue(reply.isSuccess());

        Collection<JsonRpcResponse> responses = ((ReplyResultCollection<JsonRpcResponse>) reply).getData();
        Assert.assertEquals(3, responses.size());
        for (JsonRpcResponse response : responses) {
            Assert.assertTrue(response instanceof JsonRpcResponseError);
            JsonRpcResponseError error = (JsonRpcResponseError) response;
            Assert.assertNull(error.getIdentifier());
            Assert.assertEquals(-32600, error.getCode());
            Assert.assertEquals("Invalid Request", error.getMessage());
        }
    }

    @Test
    public void testBatchExample() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                if (request.getMethod().equals("sum"))
                    return new JsonRpcResponseResult<>(request.getIdentifier(), 7);
                if (request.getMethod().equals("notify_hello"))
                    return null;
                if (request.getMethod().equals("subtract"))
                    return new JsonRpcResponseResult<>(request.getIdentifier(), 19);
                if (request.getMethod().equals("get_data"))
                    return new JsonRpcResponseResult<>(request.getIdentifier(), new Object[]{"hello", 5});
                return JsonRpcResponseError.newMethodNotFound(request.getIdentifier());
            }
        };
        TestClient client = new TestClient(server);

        String request = "[\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"sum\", \"params\": [1,2,4], \"id\": \"1\"},\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"notify_hello\", \"params\": [7]},\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"subtract\", \"params\": [42,23], \"id\": \"2\"},\n" +
                "        {\"foo\": \"boo\"},\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"foo.get\", \"params\": {\"name\": \"myself\"}, \"id\": \"5\"},\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"get_data\", \"id\": \"9\"} \n" +
                "    ]";
        JsonRpcContextBatch contextBatch = new JsonRpcContextBatch();
        contextBatch.addMethod("1", "sum");
        contextBatch.addMethod("2", "subtract");
        contextBatch.addMethod("5", "foo.get");
        contextBatch.addMethod("9", "get_data");
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), contextBatch);
        Assert.assertTrue(reply.isSuccess());

        Collection<JsonRpcResponse> responses = ((ReplyResultCollection<JsonRpcResponse>) reply).getData();
        Assert.assertEquals(5, responses.size());
        Iterator<JsonRpcResponse> iterator = responses.iterator();

        JsonRpcResponse response = iterator.next();
        Assert.assertFalse(response.isError());
        Assert.assertEquals("1", response.getIdentifier());
        Assert.assertEquals((Integer) 7, ((JsonRpcResponseResult<Integer>) response).getResult());

        response = iterator.next();
        Assert.assertFalse(response.isError());
        Assert.assertEquals("2", response.getIdentifier());
        Assert.assertEquals((Integer) 19, ((JsonRpcResponseResult<Integer>) response).getResult());

        response = iterator.next();
        Assert.assertTrue(response.isError());
        Assert.assertNull(response.getIdentifier());
        Assert.assertEquals(-32600, ((JsonRpcResponseError) response).getCode());

        response = iterator.next();
        Assert.assertTrue(response.isError());
        Assert.assertEquals("5", response.getIdentifier());
        Assert.assertEquals(-32601, ((JsonRpcResponseError) response).getCode());

        response = iterator.next();
        Assert.assertFalse(response.isError());
        Assert.assertEquals("9", response.getIdentifier());
    }

    @Test
    public void testBatchAllNotification() {
        JsonRpcServer server = new JsonRpcServerBase() {
            @Override
            public JsonRpcResponse handle(JsonRpcRequest request) {
                Assert.assertTrue(request.isNotification());
                return null;
            }
        };
        TestClient client = new TestClient(server);

        String request = "[\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"notify_sum\", \"params\": [1,2,4]},\n" +
                "        {\"jsonrpc\": \"2.0\", \"method\": \"notify_hello\", \"params\": [7]}\n" +
                "    ]";
        Reply reply = client.send(request, null);
        Assert.assertTrue(reply.isSuccess());
        reply = client.deserializeResponses(((ReplyResult<String>) reply).getData(), new JsonRpcContextBatch());
        Assert.assertEquals(ReplySuccess.instance(), reply);
    }
}
