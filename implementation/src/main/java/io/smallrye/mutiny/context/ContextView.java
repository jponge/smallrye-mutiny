package io.smallrye.mutiny.context;

import io.smallrye.mutiny.Context;

import java.util.function.Supplier;

public class ContextView implements Context {

    private final UpdatableContext updatableContext;

    public ContextView(UpdatableContext updatableContext) {
        this.updatableContext = updatableContext;
    }

    @Override
    public boolean contains(String key) {
        return updatableContext.contains(key);
    }

    @Override
    public <T> T get(String key) {
        return updatableContext.get(key);
    }

    @Override
    public <T> T getOrElse(String key, Supplier<T> alternativeSupplier) {
        return updatableContext.getOrElse(key, alternativeSupplier);
    }

    public UpdatableContext updatableContext() {
        return updatableContext;
    }

    @Override
    public String toString() {
        return updatableContext.toString();
    }
}
