package ok.dht;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DemoServer {
    public static void main(String[] args) throws Exception {
        int port = 8080;
        String url = "http://localhost:" + port;
        List<String> clusterUrls = List.of(url);
        Path outputPath = Path.of("/Users/veronika/dht/node1");
        if (!Files.exists(outputPath)) {
            Files.createDirectory(outputPath);
        }
        ServiceConfig cfg = new ServiceConfig(
                port,
                url,
                clusterUrls,
                outputPath
        );
        new ServiceImpl(cfg).start().get(1, TimeUnit.SECONDS);
        System.out.println("Socket is ready: " + url);
    }
}