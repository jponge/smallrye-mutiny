package io.smallrye.mutiny.operators.uni;

import static io.smallrye.mutiny.helpers.EmptyUniSubscription.DONE;

import io.smallrye.mutiny.operators.AbstractUni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import org.jetbrains.annotations.NotNull;

public class UniNever<T> extends AbstractUni<T> {
    public static final UniNever<Object> INSTANCE = new UniNever<>();

    private UniNever() {
        // avoid direct instantiation.
    }

    @Override
    public void subscribe(@NotNull UniSubscriber<? super T> subscriber) {
        subscriber.onSubscribe(DONE);
    }
}
