package ok.dht.streaming;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;

import java.io.IOException;

public class StreamingHttpSession extends HttpSession {

    public StreamingHttpSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    @Override
    protected void writeResponse(Response response, boolean includeBody) throws IOException {
        if (response instanceof ChunkedResponse chunkedResponse) {
            write(new StreamingQueueItem(chunkedResponse.getIterator(), chunkedResponse.toBytes(false)));
        } else {
            super.writeResponse(response, includeBody);
        }
    }
}
