/*
 * Copyright 2021 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ok.dht;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TwoNodeTest extends TestBase {
    private List<ServiceInfo> nodes;

    @BeforeEach
    public void setUp() throws Exception {
        nodes = createServices(2);
    }
    
    @AfterEach
    public void teardown() throws Exception {
        for (ServiceInfo serviceInfo : nodes) {
            serviceInfo.cleanUp();
        }
    }

    @Test
    void tooSmallRF() throws Exception {
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, nodes.get(0).get(randomId(), 0, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, nodes.get(0).upsert(randomId(), randomValue(), 0, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, nodes.get(0).delete(randomId(), 0, 2).statusCode());
    }

    @Test
    void tooBigRF() throws Exception {
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, nodes.get(0).get(randomId(), 3, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, nodes.get(0).upsert(randomId(), randomValue(), 3, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, nodes.get(0).delete(randomId(), 3, 2).statusCode());
    }

    @Test
    void unreachableRF() throws Exception {
        nodes.get(0).stop();

        assertEquals(HttpURLConnection.HTTP_GATEWAY_TIMEOUT, nodes.get(1).get(randomId(), 2, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_GATEWAY_TIMEOUT, nodes.get(1).upsert(randomId(), randomValue(), 2, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_GATEWAY_TIMEOUT, nodes.get(1).delete(randomId(), 2, 2).statusCode());
    }

    @Test
    void overlapRead() throws Exception {
        String key = randomId();

        byte[] value1 = randomValue();
        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(0).upsert(key, value1, 1, 2).statusCode());

        {
            HttpResponse<byte[]> response = nodes.get(0).get(key, 2, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value1, response.body());
        }
        {
            HttpResponse<byte[]> response = nodes.get(1).get(key, 2, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value1, response.body());
        }

        waitForVersionAdvancement();

        byte[] value2 = randomValue();
        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(1).upsert(key, value2, 1, 2).statusCode());

        {
            HttpResponse<byte[]> response = nodes.get(0).get(key, 2, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value2, response.body());
        }
        {
            HttpResponse<byte[]> response = nodes.get(1).get(key, 2, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value2, response.body());
        }
    }

    @Test
    void overlapWrite() throws Exception {
        String key = randomId();

        byte[] value1 = randomValue();
        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(0).upsert(key, value1, 2, 2).statusCode());

        {
            HttpResponse<byte[]> response = nodes.get(0).get(key, 1, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value1, response.body());
        }
        {
            HttpResponse<byte[]> response = nodes.get(1).get(key, 1, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value1, response.body());
        }

        waitForVersionAdvancement();

        byte[] value2 = randomValue();
        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(1).upsert(key, value2, 2, 2).statusCode());

        {
            HttpResponse<byte[]> response = nodes.get(0).get(key, 1, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value2, response.body());
        }
        {
            HttpResponse<byte[]> response = nodes.get(1).get(key, 1, 2);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value2, response.body());
        }
    }

    @Test
    void overlapDelete() throws Exception {
        String key = randomId();
        byte[] value = randomValue();

        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(0).upsert(key, value, 2, 2).statusCode());
        waitForVersionAdvancement();
        assertEquals(HttpURLConnection.HTTP_ACCEPTED, nodes.get(0).delete(key, 2, 2).statusCode());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, nodes.get(0).get(key, 1, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, nodes.get(1).get(key, 1, 2).statusCode());

        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(1).upsert(key, value, 2, 2).statusCode());
        waitForVersionAdvancement();
        assertEquals(HttpURLConnection.HTTP_ACCEPTED, nodes.get(1).delete(key, 2, 2).statusCode());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, nodes.get(0).get(key, 1, 2).statusCode());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, nodes.get(1).get(key, 1, 2).statusCode());
    }

    @Test
    void missedWrite() throws Exception {
        String key = randomId();

        for (int i = 0; i < nodes.size(); i++) {
            nodes.get(i).stop();

            byte[] value = randomValue();
            int status = nodes.get((i + 1) % nodes.size()).upsert(key, value, 1, 2).statusCode();
            assertEquals(HttpURLConnection.HTTP_CREATED, status);

            nodes.get(i).start();

            {
                HttpResponse<byte[]> response = nodes.get(i).get(key, 2, 2);
                assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
                assertArrayEquals(value, response.body());
            }

            waitForVersionAdvancement();
        }
    }

    @Test
    void missedDelete() throws Exception {
        String key = randomId();

        for (int i = 0; i < nodes.size(); i++) {
            byte[] value = randomValue();
            assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(i).upsert(key, value, 2, 2).statusCode());

            nodes.get(i).stop();

            waitForVersionAdvancement();

            int statusCode = nodes.get((i + 1) % nodes.size()).delete(key, 1, 2).statusCode();
            assertEquals(HttpURLConnection.HTTP_ACCEPTED, statusCode);

            nodes.get(i).start();

            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, nodes.get(i).get(key, 2, 2).statusCode());

            waitForVersionAdvancement();
        }
    }

    @Test
    void missedOverwrite() throws Exception {
        String key = randomId();

        for (int i = 0; i < nodes.size(); i++) {
            byte[] value1 = randomValue();
            assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(i).upsert(key, value1, 2, 2).statusCode());

            nodes.get(i).stop();

            waitForVersionAdvancement();

            byte[] value2 = randomValue();
            int status = nodes.get((i + 1) % nodes.size()).upsert(key, value2, 1, 2).statusCode();
            assertEquals(HttpURLConnection.HTTP_CREATED, status);

            nodes.get(i).start();

            {
                HttpResponse<byte[]> response = nodes.get(i).get(key, 2, 2);
                assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
                assertArrayEquals(value2, response.body());
            }

            waitForVersionAdvancement();
        }
    }

    @Test
    void missedRecreate() throws Exception {
        String key = randomId();

        for (int i = 0; i < nodes.size(); i++) {

            byte[] value1 = randomValue();
            assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(i).upsert(key, value1, 2, 2).statusCode());
            waitForVersionAdvancement();
            assertEquals(HttpURLConnection.HTTP_ACCEPTED, nodes.get(i).delete(key, 2, 2).statusCode());

            nodes.get(i).stop();

            waitForVersionAdvancement();

            byte[] value2 = randomValue();
            assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get((i + 1) % nodes.size()).upsert(key, value2, 1, 2).statusCode());

            nodes.get(i).start();

            {
                HttpResponse<byte[]> response = nodes.get(i).get(key, 2, 2);
                assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
                assertArrayEquals(value2, response.body());
            }

            waitForVersionAdvancement();
        }
    }

    @Test
    void tolerateFailure() throws Exception {
        String key = randomId();

        for (int i = 0; i < nodes.size(); i++) {

            byte[] value = randomValue();
            assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(i).upsert(key, value, 2, 2).statusCode());

            nodes.get(i).stop();

            {
                HttpResponse<byte[]> response = nodes.get((i + 1) % nodes.size()).get(key, 1, 2);
                assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
                assertArrayEquals(value, response.body());
            }

            waitForVersionAdvancement();

            assertEquals(HttpURLConnection.HTTP_ACCEPTED, nodes.get((i + 1) % nodes.size()).delete(key, 1, 2).statusCode());

            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, nodes.get((i + 1) % nodes.size()).get(key, 1, 2).statusCode());

            nodes.get(i).start();
            waitForVersionAdvancement();
        }
    }

    @Test
    void respectRF() throws Exception {
        String key = randomId();
        byte[] value = randomValue();

        assertEquals(HttpURLConnection.HTTP_CREATED, nodes.get(0).upsert(key, value, 1, 1).statusCode());

        for (ServiceInfo node : nodes) {
            node.stop();
        }

        int successCount = 0;
        for (ServiceInfo serviceInfo : nodes) {
            serviceInfo.start();

            HttpResponse<byte[]> response = serviceInfo.get(key, 1, 1);
            if (response.statusCode() == HttpURLConnection.HTTP_OK && Arrays.equals(value, response.body())) {
                successCount++;
            }

            serviceInfo.stop();
        }
        assertEquals(1, successCount);
    }
}
