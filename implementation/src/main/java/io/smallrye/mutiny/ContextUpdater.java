package io.smallrye.mutiny;

public interface ContextUpdater {

    ContextUpdater put(String key, Object value);

    ContextUpdater delete(String key);
}
