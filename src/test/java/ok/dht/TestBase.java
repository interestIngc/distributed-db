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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TestBase {
    private static final int VALUE_LENGTH = 1024;

    protected final HttpClient client = HttpClient.newHttpClient();

    protected static String randomId() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    protected void waitForVersionAdvancement() throws Exception {
        long ms = System.currentTimeMillis();
        while (ms == System.currentTimeMillis()) {
            Thread.sleep(1);
        }
    }

    protected static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    protected static List<ServiceInfo> createServices(int clusterSize) throws Exception {
        int[] ports = randomPorts(clusterSize);
        Arrays.sort(ports);
        List<String> cluster = new ArrayList<>(clusterSize);
        for (int port : ports) {
            cluster.add("http://localhost:" + port);
        }

        List<ServiceInfo> services = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            Path workingDir = Files.createTempDirectory("service" + i);

            ServiceConfig config = new ServiceConfig(ports[i], cluster.get(i), cluster, workingDir);
            ServiceInfo serviceInfo =
                    new ServiceInfo(
                            new ServiceImpl(config),
                            config,
                            HttpClient.newHttpClient()
                    );
            serviceInfo.start();

            services.add(serviceInfo);
        }

        return services;
    }

    private static int[] randomPorts(int count) {
        List<ServerSocket> sockets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            try {
                sockets.add(new ServerSocket());
            } catch (IOException e) {
                throw closeAndRethrow(sockets, e);
            }
        }

        int[] ports = new int[count];
        for (int i = 0; i < count; i++) {
            try {
                ServerSocket socket = sockets.get(i);
                socket.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 0), 1);
                ports[i] = socket.getLocalPort();
            } catch (IOException e) {
                throw closeAndRethrow(sockets, e);
            }
        }

        for (ServerSocket socket : sockets) {
            try {
                socket.close();
            } catch (IOException e) {
                throw closeAndRethrow(sockets, e);
            }
        }

        return ports;
    }

    private static RuntimeException closeAndRethrow(List<ServerSocket> sockets, IOException e) {
        UncheckedIOException ex = new UncheckedIOException("Can't discover a free port", e);
        for (ServerSocket socket : sockets) {
            try {
                socket.close();
            } catch (IOException e2) {
                ex.addSuppressed(e2);
            }
        }
        throw ex;
    }
}
