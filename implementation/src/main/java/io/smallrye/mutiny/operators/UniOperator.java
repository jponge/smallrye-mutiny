package io.smallrye.mutiny.operators;

import org.jspecify.annotations.Nullable;

import io.smallrye.mutiny.Uni;

public abstract class UniOperator<I extends @Nullable Object, O extends @Nullable Object> extends AbstractUni<O> {

    private final Uni<? extends I> upstream;

    public UniOperator(Uni<? extends I> upstream) {
        // NOTE: upstream can be null. It's null when creating a "source".
        this.upstream = upstream;
    }

    public Uni<? extends I> upstream() {
        return upstream;
    }

}
