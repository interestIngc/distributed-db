package ok.dht.dao;

public class StorageClosedException extends RuntimeException {
    public StorageClosedException(Throwable causedBy) {
        super(causedBy);
    }
}
