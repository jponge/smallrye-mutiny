package io.smallrye.mutiny;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import io.smallrye.mutiny.context.UpdatableContext;

public interface Context {

    boolean contains(String key);

    <T> T get(String key);

    <T> T getOrElse(String key, Supplier<T> alternativeSupplier);

    static Context empty() {
        return new UpdatableContext().toContextView();
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
        return new UpdatableContext(map).toContextView();
    }

    static Context from(Map<String, Object> entries) {
        return new UpdatableContext(nonNull(entries, "entries")).toContextView();
    }
}
