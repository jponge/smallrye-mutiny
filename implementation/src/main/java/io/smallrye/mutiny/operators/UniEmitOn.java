package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

public class UniEmitOn<I> extends UniOperator<I, I> {
    private final Executor executor;

    UniEmitOn(Uni<I> upstream, Executor executor) {
        super(upstream);
        this.executor = nonNull(executor, "executor");
    }

    @Override
    protected void subscribing(UniSubscriber<? super I> subscriber) {
        AbstractUni.subscribe(upstream(), new UniDelegatingSubscriber<I, I>(subscriber) {

            private final AtomicBoolean done = new AtomicBoolean();

            @Override
            public void onSubscribe(UniSubscription subscription) {
                super.onSubscribe(() -> {
                    if (done.compareAndSet(false, true)) {
                        subscription.cancel();
                    }
                });
            }

            @Override
            public void onItem(I item) {
                if (done.compareAndSet(false, true)) {
                    executor.execute(() -> subscriber.onItem(item));
                }
            }

            @Override
            public void onFailure(Throwable failure) {
                if (done.compareAndSet(false, true)) {
                    executor.execute(() -> subscriber.onFailure(failure));
                } else {
                    Infrastructure.handleDroppedException(failure);
                }
            }
        });
    }
}
