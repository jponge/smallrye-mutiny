package io.smallrye.mutiny.context;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.smallrye.mutiny.Context;

public class ContextImpl implements Context, Context.Updater {

    private volatile ConcurrentHashMap<String, Object> entries;

    private ConcurrentHashMap<String, Object> getEntries() {
        if (entries == null) {
            synchronized (this) {
                if (entries == null) {
                    entries = new ConcurrentHashMap<>(4);
                }
            }
        }
        return entries;
    }

    public ContextImpl() {
        // Nothing to do, keep _entries null
    }

    public ContextImpl(Map<String, Object> initialEntries) {
        entries = new ConcurrentHashMap<>(initialEntries);
    }

    @Override
    public boolean contains(String key) {
        if (entries == null) {
            return false;
        }
        return getEntries().containsKey(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        if (entries == null) {
            throw new NoSuchElementException("There is no context entry for key " + key);
        }
        Object value = getEntries().get(key);
        if (value == null) {
            throw new NoSuchElementException("There is no context entry for key " + key);
        }
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getOrElse(String key, Supplier<T> alternativeSupplier) {
        if (entries == null) {
            return alternativeSupplier.get();
        }
        Object value = getEntries().get(key);
        if (value == null) {
            return alternativeSupplier.get();
        } else {
            return (T) value;
        }
    }

    @Override
    public Updater put(String key, Object value) {
        getEntries().put(key, value);
        return this;
    }

    @Override
    public Updater delete(String key) {
        getEntries().remove(key);
        return this;
    }

    @Override
    public String toString() {
        return "ContextImpl{" +
                "entries=" + entries +
                '}';
    }
}
