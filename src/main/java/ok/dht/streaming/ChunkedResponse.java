package ok.dht.streaming;

import jdk.incubator.foreign.MemorySegment;
import ok.dht.dao.Entry;
import one.nio.http.Response;

import java.util.Iterator;

public class ChunkedResponse extends Response {
    private final Iterator<Entry<MemorySegment>> iterator;

    public ChunkedResponse(Iterator<Entry<MemorySegment>> iterator) {
        super(OK);
        addHeader("Transfer-Encoding: chunked");
        this.iterator = iterator;
    }

    public Iterator<Entry<MemorySegment>> getIterator() {
        return iterator;
    }
}
