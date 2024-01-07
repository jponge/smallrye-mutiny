package io.smallrye.mutiny.operators.multi;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

/**
 * ConcatMap operator without prefetching items from the upstream.
 * Requests are forwarded lazily to the upstream when:
 * <ul>
 * <li>First downstream request.</li>
 * <li>The inner has no more outstanding requests.</li>
 * <li>The inner completed without emitting items or with outstanding requests.</li>
 * </ul>
 * <p>
 * This operator can collect failures and postpone them until termination.
 *
 * @param <I> the upstream value type / input type
 * @param <O> the output value type / produced type
 */
public class MultiConcatMapOp<I, O> extends AbstractMultiOperator<I, O> {

    private final Function<? super I, ? extends Publisher<? extends O>> mapper;

    private final boolean postponeFailurePropagation;

    public MultiConcatMapOp(Multi<? extends I> upstream,
            Function<? super I, ? extends Publisher<? extends O>> mapper,
            boolean postponeFailurePropagation) {
        super(upstream);
        this.mapper = mapper;
        this.postponeFailurePropagation = postponeFailurePropagation;
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("The subscriber must not be `null`");
        }
        MainSubscriber<? super I, O> sub = new MainSubscriber<>(mapper, postponeFailurePropagation, subscriber);
        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, sub));
    }

    private enum SubscriberState {
        INIT,
        ACTIVE,
        DONE
    }

    private static class MainSubscriber<I, O> implements MultiSubscriber<I>, Flow.Subscription, ContextSupport {

        private final Function<? super I, ? extends Publisher<? extends O>> mapper;
        private final boolean postponeFailurePropagation;
        private final MultiSubscriber<? super O> downstream;

        private final AtomicLong demand = new AtomicLong();
        private final AtomicReference<Flow.Subscription> mainUpstream = new AtomicReference<>();
        private final AtomicReference<SubscriberState> mainSubscriberState = new AtomicReference<>(SubscriberState.INIT);
        private final InnerSubscriber innerSubscriber = new InnerSubscriber();
        private Throwable failure;

        private MainSubscriber(Function<? super I, ? extends Publisher<? extends O>> mapper, boolean postponeFailurePropagation,
                MultiSubscriber<? super O> downstream) {
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (mainUpstream.compareAndSet(null, subscription)) {
                mainSubscriberState.set(SubscriberState.ACTIVE);
                downstream.onSubscribe(this);
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onItem(I item) {
            if (mainSubscriberState.get() == SubscriberState.DONE) {
                return;
            }
            try {
                Publisher<? extends O> publisher = requireNonNull(mapper.apply(item), "The mapper produced a null publisher");
                innerSubscriber.state.set(SubscriberState.INIT);
                publisher.subscribe(innerSubscriber);
            } catch (Throwable err) {
                mainUpstream.get().cancel();
                mainSubscriberState.set(SubscriberState.DONE);
                downstream.onFailure(addFailure(err));
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (mainSubscriberState.getAndSet(SubscriberState.DONE) != SubscriberState.DONE) {
                downstream.onFailure(addFailure(failure));
            }
        }

        @Override
        public void onCompletion() {
            if (mainSubscriberState.getAndSet(SubscriberState.DONE) == SubscriberState.DONE) {
                return;
            }
            if (innerSubscriber.state.get() != SubscriberState.ACTIVE) {
                terminate();
            }
        }

        @Override
        public void request(long n) {
            if (mainSubscriberState.get() == SubscriberState.DONE && innerSubscriber.state.get() == SubscriberState.DONE) {
                return;
            }
            if (n <= 0) {
                mainSubscriberState.set(SubscriberState.DONE);
                downstream.onFailure(Subscriptions.getInvalidRequestException());
                return;
            }
            Subscriptions.add(demand, n);
            switch (innerSubscriber.state.get()) {
                case INIT:
                case DONE:
                    mainUpstream.get().request(1L);
                    break;
                case ACTIVE:
                    innerSubscriber.upstream.get().request(n);
                    break;
            }
        }

        @Override
        public void cancel() {
            if (mainSubscriberState.compareAndSet(SubscriberState.ACTIVE, SubscriberState.DONE)) {
                mainUpstream.get().cancel();
            }
            if (innerSubscriber.state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.DONE)) {
                innerSubscriber.upstream.get().cancel();
            }
        }

        @Override
        public Context context() {
            if (downstream instanceof ContextSupport) {
                return ((ContextSupport) downstream).context();
            } else {
                return Context.empty();
            }
        }

        private Throwable addFailure(Throwable failure) {
            if (this.failure != null) {
                if (this.failure instanceof CompositeException) {
                    this.failure = new CompositeException((CompositeException) this.failure, failure);
                } else {
                    this.failure = new CompositeException(this.failure, failure);
                }
            } else {
                this.failure = failure;
            }
            return this.failure;
        }

        private void terminate() {
            if (failure != null) {
                downstream.onFailure(failure);
            } else {
                downstream.onCompletion();
            }
        }

        private class InnerSubscriber implements MultiSubscriber<O>, ContextSupport {

            private final AtomicReference<SubscriberState> state = new AtomicReference<>(SubscriberState.INIT);
            private final AtomicReference<Flow.Subscription> upstream = new AtomicReference<>();

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (state.compareAndSet(SubscriberState.INIT, SubscriberState.ACTIVE)) {
                    upstream.set(subscription);
                    long n = demand.get();
                    if (n > 0L) {
                        subscription.request(n);
                    }
                }
            }

            @Override
            public void onItem(O item) {
                if (state.get() == SubscriberState.ACTIVE) {
                    demand.decrementAndGet();
                    downstream.onItem(item);
                }
            }

            @Override
            public void onFailure(Throwable failure) {
                if (state.getAndSet(SubscriberState.DONE) == SubscriberState.DONE) {
                    return;
                }
                Throwable throwable = addFailure(failure);
                if (postponeFailurePropagation) {
                    if (mainSubscriberState.get() == SubscriberState.DONE) {
                        terminate();
                    } else {
                        mainUpstream.get().request(1L);
                    }
                } else {
                    mainSubscriberState.set(SubscriberState.DONE);
                    mainUpstream.get().cancel();
                    downstream.onFailure(throwable);
                }
            }

            @Override
            public void onCompletion() {
                if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.DONE)) {
                    if (mainSubscriberState.get() == SubscriberState.DONE) {
                        terminate();
                    } else if (demand.get() > 0L) {
                        mainUpstream.get().request(1L);
                    }
                }
            }

            @Override
            public Context context() {
                if (downstream instanceof ContextSupport) {
                    return ((ContextSupport) downstream).context();
                } else {
                    return Context.empty();
                }
            }
        }
    }
}
