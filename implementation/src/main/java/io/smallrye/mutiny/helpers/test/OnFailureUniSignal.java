package io.smallrye.mutiny.helpers.test;

import org.jetbrains.annotations.NotNull;

/**
 * A onFailure signal.
 */
public final class OnFailureUniSignal implements UniSignal {
    private final Throwable failure;

    public OnFailureUniSignal(Throwable failure) {
        this.failure = failure;
    }

    @Override
    public Throwable value() {
        return failure;
    }

    @NotNull
    @Override
    public String toString() {
        return "OnFailureSignal{" +
                "failure=" + failure +
                '}';
    }
}
