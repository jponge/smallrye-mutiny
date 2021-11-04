package io.smallrye.mutiny;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

// TODO
public final class Context {

    private volatile ConcurrentHashMap<String, Object> entries;

    private Context() {
        this.entries = null;
    }

    private Context(Map<String, Object> initialEntries) {
        this.entries = new ConcurrentHashMap<>(initialEntries);
    }

    public boolean contains(String key) {
        if (entries == null) {
            return false;
        } else {
            return entries.containsKey(key);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        if (entries == null) {
            throw new NoSuchElementException("The context is empty");
        }
        T value = (T) entries.get(key);
        if (value == null) {
            throw new NoSuchElementException("The context does not have a value for key " + key);
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOrElse(String key, Supplier<? extends T> alternativeSupplier) {
        if (entries != null) {
            T value = (T) entries.get(key);
            if (value != null) {
                return value;
            }
        }
        return alternativeSupplier.get();
    }

    public Context put(String key, Object value) {
        if (entries == null) {
            synchronized (this) {
                if (entries == null) {
                    this.entries = new ConcurrentHashMap<>(8);
                }
            }
        }
        entries.put(key, value);
        return this;
    }

    public Context delete(String key) {
        if (entries != null) {
            entries.remove(key);
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Context context = (Context) o;
        return Objects.equals(entries, context.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    @Override
    public String toString() {
        return "Context{" +
                "entries=" + entries +
                '}';
    }

    public static Context empty() {
        return new Context();
    }

    public static Context of(Object... entries) {
        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be balanced to form (key, value) pairs");
        }
        HashMap<String, Object> map = new HashMap<>();
        for (int i = 0; i < entries.length; i = i + 2) {
            String key = nonNull(entries[i], "key").toString();
            Object value = nonNull(entries[i + 1], "value");
            map.put(key, value);
        }
        return new Context(map);
    }

    public static Context from(Map<String, Object> entries) {
        return new Context(entries);
    }
}
