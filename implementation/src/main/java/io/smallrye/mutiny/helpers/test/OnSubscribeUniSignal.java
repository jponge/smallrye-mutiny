package io.smallrye.mutiny.helpers.test;

import io.smallrye.mutiny.subscription.UniSubscription;
import org.jetbrains.annotations.NotNull;

/**
 * A onSubscribe signal.
 */
public final class OnSubscribeUniSignal implements UniSignal {
    private final UniSubscription subscription;

    public OnSubscribeUniSignal(UniSubscription subscription) {
        this.subscription = subscription;
    }

    @Override
    public UniSubscription value() {
        return subscription;
    }

    @NotNull
    @Override
    public String toString() {
        return "OnSubscribeSignal{" +
                "subscription=" + subscription +
                '}';
    }
}
