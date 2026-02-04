package io.smallrye.mutiny.helpers.test;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A cancellation signal.
 */
public final class OnCancellationUniSignal implements UniSignal {

    @Nullable
    @Override
    public Void value() {
        return null;
    }

    @NotNull
    @Override
    public String toString() {
        return "OnCancellationSignal{}";
    }
}
