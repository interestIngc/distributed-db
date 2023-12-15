package ok.dht.dao;

public interface Entry<D> {
    D key();

    D value();

    D timestamp();

    default boolean isTombstone() {
        return value() == null;
    }
}
