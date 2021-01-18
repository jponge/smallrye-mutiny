package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

public class UniOnCancellation<T> extends UniOperator<T, T> {
    private final Runnable callback;

    public UniOnCancellation(Uni<T> upstream, Runnable callback) {
        super(nonNull(upstream, "upstream"));
        this.callback = nonNull(callback, "callback");
    }

    @Override
    protected void subscribing(UniSubscriber<? super T> subscriber) {
        AbstractUni.subscribe(upstream(), new UniDelegatingSubscriber<T, T>(subscriber) {

            private final AtomicBoolean done = new AtomicBoolean();

            @Override
            public void onSubscribe(UniSubscription subscription) {
                super.onSubscribe(() -> {
                    if (done.compareAndSet(false, true)) {
                        callback.run();
                        subscription.cancel();
                    }
                });
            }

            @Override
            public void onItem(T item) {
                if (done.compareAndSet(false, true)) {
                    super.onItem(item);
                }
            }

            @Override
            public void onFailure(Throwable failure) {
                if (done.compareAndSet(false, true)) {
                    super.onFailure(failure);
                } else {
                    Infrastructure.handleDroppedException(failure);
                }
            }
        });
    }
}
