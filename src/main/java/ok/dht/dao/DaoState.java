package ok.dht.dao;

public class DaoState {
    final Config config;
    final DaoMemory memory;
    final DaoMemory flushing;
    final Storage storage;
    final boolean closed;

    DaoState(Config config, DaoMemory memory, DaoMemory flushing, Storage storage) {
        this.config = config;
        this.memory = memory;
        this.flushing = flushing;
        this.storage = storage;
        this.closed = false;
    }

    DaoState(Config config, Storage storage, boolean closed) {
        this.config = config;
        this.memory = DaoMemory.EMPTY;
        this.flushing = DaoMemory.EMPTY;
        this.storage = storage;
        this.closed = closed;
    }

    static DaoState newState(Config config, Storage storage) {
        return new DaoState(
                config,
                new DaoMemory(config.flushThresholdBytes()),
                DaoMemory.EMPTY,
                storage
        );
    }

    public DaoState prepareForFlush() {
        checkNotClosed();
        if (isFlushing()) {
            throw new IllegalStateException("Already flushing");
        }
        return new DaoState(
                config,
                new DaoMemory(config.flushThresholdBytes()),
                memory,
                storage
        );
    }

    public DaoState afterCompact(Storage storage) {
        checkNotClosed();
        return new DaoState(
                config,
                memory,
                flushing,
                storage
        );
    }

    public DaoState afterFlush(Storage storage) {
        checkNotClosed();
        if (!isFlushing()) {
            throw new IllegalStateException("Wasn't flushing");
        }
        return new DaoState(
                config,
                memory,
                DaoMemory.EMPTY,
                storage
        );
    }

    public DaoState afterClosed() {
        checkNotClosed();
        if (!storage.isClosed()) {
            throw new IllegalStateException("Storage should be closed early");
        }
        return new DaoState(config, storage, true);
    }

    public void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
    }

    public boolean isFlushing() {
        return this.flushing != DaoMemory.EMPTY;
    }
}
