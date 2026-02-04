package io.smallrye.mutiny.helpers.test;

import org.jetbrains.annotations.Nullable;

/**
 * A signal: onSubscribe, onItem, onFailure or cancel.
 */
public interface UniSignal {

    /**
     * Get the signal associated value, if any.
     *
     * @return the value
     */
    @Nullable Object value();
}
