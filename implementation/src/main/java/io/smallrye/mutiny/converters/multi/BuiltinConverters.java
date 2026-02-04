package io.smallrye.mutiny.converters.multi;

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
}
