package ok.dht.dao;

public record BaseEntry<Data>(Data key, Data value, Data timestamp) implements Entry<Data> {
    @Override
    public String toString() {
        return "{" + key + ":" + value + ":" + timestamp + "}";
    }
}
