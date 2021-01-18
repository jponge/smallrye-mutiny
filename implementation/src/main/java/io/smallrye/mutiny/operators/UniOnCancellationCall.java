package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

public class UniOnCancellationCall<I> extends UniOperator<I, I> {

    private final Supplier<Uni<?>> supplier;

    public UniOnCancellationCall(Uni<? extends I> upstream, Supplier<Uni<?>> supplier) {
        super(nonNull(upstream, "upstream"));
        this.supplier = nonNull(supplier, "supplier");
    }

    @Override
    protected void subscribing(UniSubscriber<? super I> downstream) {
        upstream().subscribe().withSubscriber(new Subscription(downstream));

        //        upstream().subscribe().withSubscriber(new UniDelegatingSubscriber<I, I>(subscriber) {
        //
        //            private final AtomicBoolean done = new AtomicBoolean();
        //
        //            @Override
        //            public void onItem(I item) {
        //                if (done.compareAndSet(false, true)) {
        //                    super.onItem(item);
        //                }
        //            }
        //
        //            @Override
        //            public void onFailure(Throwable failure) {
        //                if (done.compareAndSet(false, true)) {
        //                    super.onFailure(failure);
        //                } else {
        //                    Infrastructure.handleDroppedException(failure);
        //                }
        //            }
        //
        //            @Override
        //            public void onSubscribe(UniSubscription subscription) {
        //                subscriber.onSubscribe(new UniSubscription() {
        //
        //                    @Override
        //                    public void cancel() {
        //                        if (done.compareAndSet(false, true)) {
        //                            execute().subscribe().with(
        //                                    ignoredItem -> subscription.cancel(),
        //                                    ignoredException -> {
        //                                        Infrastructure.handleDroppedException(ignoredException);
        //                                        subscription.cancel();
        //                                    });
        //                        }
        //                    }
        //
        //                    private Uni<?> execute() {
        //                        try {
        //                            return nonNull(supplier.get(), "uni");
        //                        } catch (Throwable err) {
        //                            return Uni.createFrom().failure(err);
        //                        }
        //                    }
        //                });
        //            }
        //        });
    }

    private class Subscription implements UniSubscriber<I>, UniSubscription {

        private final AtomicBoolean done;
        private final UniSubscriber<? super I> downstream;
        private volatile UniSubscription subscription;

        public Subscription(UniSubscriber<? super I> downstream) {
            this.downstream = downstream;
            done = new AtomicBoolean();
        }

        @Override
        public void onSubscribe(UniSubscription subscription) {
            if (done.get()) {
                if (this.subscription == null) {
                    doCancel();
                } else {
                    downstream.onFailure(new IllegalStateException("The subscription has already completed"));
                }
            } else {
                this.subscription = subscription;
                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onItem(I item) {
            if (done.compareAndSet(false, true)) {
                downstream.onItem(item);
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (done.compareAndSet(false, true)) {
                downstream.onFailure(failure);
            } else {
                Infrastructure.handleDroppedException(failure);
            }
        }

        @Override
        public void cancel() {
            if (done.compareAndSet(false, true)) {
                if (subscription != null) {
                    doCancel();
                }
            }
        }

        private void doCancel() {
            Uni<?> uni;
            try {
                uni = nonNull(supplier.get(), "uni");
            } catch (Throwable err) {
                uni = Uni.createFrom().failure(err);
            }
            uni.subscribe().with(
                    ignoredItem -> subscription.cancel(),
                    ignoredFailure -> {
                        Infrastructure.handleDroppedException(ignoredFailure);
                        subscription.cancel();
                    });
        }
    }
}
