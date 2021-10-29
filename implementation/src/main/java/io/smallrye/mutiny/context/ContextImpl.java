package io.smallrye.mutiny.context;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.smallrye.mutiny.Context;

public class ContextImpl implements Context, Context.Updater {

    private volatile ConcurrentHashMap<String, Object> _entries;

    private ConcurrentHashMap<String, Object> entries() {
        if (_entries == null) {
            synchronized (this) {
                if (_entries == null) {
                    _entries = new ConcurrentHashMap<>(4);
                }
            }
        }
        return _entries;
    }

    public ContextImpl() {
        // Nothing to do, keep _entries null
    }

    public ContextImpl(Map<String, Object> initialEntries) {
        _entries = new ConcurrentHashMap<>(initialEntries);
    }

    @Override
    public boolean contains(String key) {
        return entries().containsKey(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        Object value = entries().get(key);
        if (value == null) {
            throw new NoSuchElementException("There is no context entry for key " + key);
        }
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getOrElse(String key, Supplier<T> alternativeSupplier) {
        Object value = entries().get(key);
        if (value == null) {
            return alternativeSupplier.get();
        } else {
            return (T) value;
        }
    }

    @Override
    public Updater put(String key, Object value) {
        entries().put(key, value);
        return this;
    }

    @Override
    public Updater delete(String key) {
        entries().remove(key);
        return this;
    }

    @Override
    public String toString() {
        return "ContextImpl{" +
                "_entries=" + _entries +
                '}';
    }
}
