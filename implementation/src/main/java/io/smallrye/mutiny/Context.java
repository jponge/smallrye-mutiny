package io.smallrye.mutiny;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.HashMap;
import java.util.function.Supplier;

import io.smallrye.mutiny.context.ContextImpl;

public interface Context {

    interface Updater {

        Updater put(String key, Object value);

        Updater delete(String key);
    }

    boolean contains(String key);

    <T> T get(String key);

    <T> T getOrElse(String key, Supplier<T> alternativeSupplier);

    static Context empty() {
        return new ContextImpl();
    }

    static Context of(Object... entries) {
        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be balanced to form (key, value) pairs");
        }
        HashMap<String, Object> map = new HashMap<>();
        for (int i = 0; i < entries.length; i = i + 2) {
            Object key = nonNull(entries[i], "key");
            Object value = nonNull(entries[i + 1], "value");
            map.put(key.toString(), value);
        }
        return new ContextImpl(map);
    }
}
