package io.smallrye.mutiny.converters.uni;

import org.jetbrains.annotations.NotNull;

public class BuiltinConverters {
    private BuiltinConverters() {
        // Avoid direct instantiation
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static <T> FromCompletionStage<T> fromCompletionStage() {
        return FromCompletionStage.INSTANCE;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static <T> ToCompletionStage<T> toCompletionStage() {
        return ToCompletionStage.INSTANCE;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static <T> ToCompletableFuture<T> toCompletableFuture() {
        return ToCompletableFuture.INSTANCE;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static <T> ToPublisher<T> toPublisher() {
        return ToPublisher.INSTANCE;
    }
}
