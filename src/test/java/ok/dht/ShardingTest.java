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

class ShardingTest extends TestBase {
    private List<ServiceInfo> serviceInfos;

    @BeforeEach
    public void setUp() throws Exception {
        serviceInfos = createServices(2);
    }

    @AfterEach
    public void teardown() throws Exception {
        for (ServiceInfo serviceInfo : serviceInfos) {
            serviceInfo.cleanUp();
        }
    }

    @Test
    void insert() throws Exception {
        String key = "key";
        byte[] value = randomValue();

        for (ServiceInfo insertService : serviceInfos) {
            assertEquals(HttpURLConnection.HTTP_CREATED, insertService.upsert(key, value).statusCode());

            for (ServiceInfo getService : serviceInfos) {
                HttpResponse<byte[]> response = getService.get(key);
                assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
                assertArrayEquals(value, response.body());
            }
        }
    }

    @Test
    void insertEmpty() throws Exception {
        String key = randomId();
        byte[] value = new byte[0];

        for (ServiceInfo insertService : serviceInfos) {
            assertEquals(HttpURLConnection.HTTP_CREATED, insertService.upsert(key, value).statusCode());

            for (ServiceInfo getService : serviceInfos) {
                HttpResponse<byte[]> response = getService.get(key);
                assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
                assertArrayEquals(value, response.body());
            }
        }
    }

    @Test
    void lifecycle2keys() throws Exception {
        String key1 = randomId();
        byte[] value1 = randomValue();

        String key2 = randomId();
        byte[] value2 = randomValue();

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(0).upsert(key1, value1).statusCode());

        assertArrayEquals(value1, serviceInfos.get(0).get(key1).body());
        assertArrayEquals(value1, serviceInfos.get(1).get(key1).body());

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(1).upsert(key2, value2).statusCode());

        assertArrayEquals(value1, serviceInfos.get(0).get(key1).body());
        assertArrayEquals(value1, serviceInfos.get(1).get(key1).body());
        assertArrayEquals(value2, serviceInfos.get(0).get(key2).body());
        assertArrayEquals(value2, serviceInfos.get(1).get(key2).body());

        assertEquals(HttpURLConnection.HTTP_ACCEPTED, serviceInfos.get(0).delete(key1).statusCode());
        assertEquals(HttpURLConnection.HTTP_ACCEPTED, serviceInfos.get(1).delete(key1).statusCode());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, serviceInfos.get(0).get(key1).statusCode());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, serviceInfos.get(1).get(key1).statusCode());
        assertArrayEquals(value2, serviceInfos.get(0).get(key2).body());
        assertArrayEquals(value2, serviceInfos.get(1).get(key2).body());

        assertEquals(HttpURLConnection.HTTP_ACCEPTED, serviceInfos.get(0).delete(key2).statusCode());
        assertEquals(HttpURLConnection.HTTP_ACCEPTED, serviceInfos.get(1).delete(key2).statusCode());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, serviceInfos.get(0).get(key2).statusCode());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, serviceInfos.get(1).get(key2).statusCode());
    }

    @Test
    void upsert() throws Exception {
        String key = randomId();
        byte[] value1 = randomValue();
        byte[] value2 = randomValue();

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(0).upsert(key, value1).statusCode());

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(1).upsert(key, value2).statusCode());

        for (ServiceInfo serviceInfo : serviceInfos) {
            HttpResponse<byte[]> response = serviceInfo.get(key);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(value2, response.body());
        }
    }

    @Test
    void upsertEmpty() throws Exception {
        String key = randomId();
        byte[] value = randomValue();
        byte[] empty = new byte[0];

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(0).upsert(key, value).statusCode());

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(0).upsert(key, empty).statusCode());

        for (ServiceInfo serviceInfo : serviceInfos) {
            HttpResponse<byte[]> response = serviceInfo.get(key);
            assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
            assertArrayEquals(empty, response.body());
        }
    }

    @Test
    void delete() throws Exception {
        String key = randomId();
        byte[] value = randomValue();

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(0).upsert(key, value).statusCode());
        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(1).upsert(key, value).statusCode());

        assertEquals(HttpURLConnection.HTTP_ACCEPTED, serviceInfos.get(0).delete(key).statusCode());

        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, serviceInfos.get(0).get(key).statusCode());
        assertEquals(HttpURLConnection.HTTP_NOT_FOUND, serviceInfos.get(1).get(key).statusCode());
    }

    @Test
    void distribute() throws Exception {
        final String key = randomId();
        final byte[] value = randomValue();

        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(0).upsert(key, value, 1, 1).statusCode());
        assertEquals(HttpURLConnection.HTTP_CREATED, serviceInfos.get(1).upsert(key, value, 1, 1).statusCode());

        for (ServiceInfo serviceInfo : serviceInfos) {
            serviceInfo.stop();
        }

        int successCount = 0;
        for (ServiceInfo serviceInfo : serviceInfos) {
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
