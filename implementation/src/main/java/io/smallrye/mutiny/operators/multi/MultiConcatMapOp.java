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

    private enum State {
        INIT,
        READY,
        PUBLISHER_REQUESTED,
        EMITTING,
        EMITTING_FINAL,
        DONE,
        CANCELLED
    }

    private static class MainSubscriber<I, O> implements MultiSubscriber<I>, Flow.Subscription, ContextSupport {

        private final Function<? super I, ? extends Publisher<? extends O>> mapper;
        private final boolean postponeFailurePropagation;
        private final MultiSubscriber<? super O> downstream;

        private final AtomicLong demand = new AtomicLong();
        private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
        private final InnerSubscriber innerSubscriber = new InnerSubscriber();
        private volatile Throwable failure;
        private Flow.Subscription mainUpstream;
        private volatile Flow.Subscription innerUpstream;

        private MainSubscriber(Function<? super I, ? extends Publisher<? extends O>> mapper, boolean postponeFailurePropagation,
                MultiSubscriber<? super O> downstream) {
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (state.compareAndSet(State.INIT, State.READY)) {
                mainUpstream = subscription;
                downstream.onSubscribe(this);
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onItem(I item) {
            if (state.get() == State.CANCELLED) {
                return;
            }
            if (state.compareAndSet(State.PUBLISHER_REQUESTED, State.EMITTING)) {
                try {
                    Publisher<? extends O> publisher = requireNonNull(mapper.apply(item),
                            "The mapper produced a null publisher");
                    publisher.subscribe(innerSubscriber);
                } catch (Throwable err) {
                    state.set(State.CANCELLED);
                    mainUpstream.cancel();
                    downstream.onFailure(addFailure(err));
                }
            }
        }

        private void innerOnItem(O item) {
            if (state.get() != State.CANCELLED) {
                demand.decrementAndGet();
                downstream.onItem(item);
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (state.getAndSet(State.CANCELLED) != State.CANCELLED) {
                if (innerUpstream != null) {
                    innerUpstream.cancel();
                }
                downstream.onFailure(addFailure(failure));
            }
        }

        private void innerOnFailure(Throwable failure) {
            Throwable throwable = addFailure(failure);
            switch (state.get()) {
                case EMITTING:
                    if (postponeFailurePropagation) {
                        if (demand.get() > 0L) {
                            state.set(State.PUBLISHER_REQUESTED);
                            mainUpstream.request(1L);
                        } else {
                            state.set(State.READY);
                        }
                    } else {
                        state.set(State.CANCELLED);
                        mainUpstream.cancel();
                        downstream.onFailure(throwable);
                    }
                    break;
                case EMITTING_FINAL:
                    state.set(State.CANCELLED);
                    mainUpstream.cancel();
                    downstream.onFailure(throwable);
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

        @Override
        public void onCompletion() {
            synchronized (innerSubscriber) {
                switch (state.get()) {
                    case EMITTING:
                        state.set(State.EMITTING_FINAL);
                        break;
                    case READY:
                    case PUBLISHER_REQUESTED:
                        terminate();
                        break;
                    default:
                        break;
                }
            }
        }

        private void innerOnCompletion() {
            synchronized (innerSubscriber) {
                switch (state.get()) {
                    case EMITTING:
                        if (demand.get() > 0L) {
                            state.set(State.PUBLISHER_REQUESTED);
                            mainUpstream.request(1L);
                        } else {
                            state.set(State.READY);
                        }
                        break;
                    case EMITTING_FINAL:
                        terminate();
                        break;
                }
            }
        }

        private void terminate() {
            if (failure != null) {
                state.set(State.CANCELLED);
                downstream.onFailure(failure);
            } else {
                state.set(State.DONE);
                downstream.onCompletion();
            }
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                state.set(State.CANCELLED);
                downstream.onFailure(Subscriptions.getInvalidRequestException());
            } else {
                Subscriptions.add(demand, n);
                boolean retry;
                do {
                    retry = false;
                    switch (state.get()) {
                        case EMITTING:
                        case EMITTING_FINAL:
                            innerUpstream.request(n);
                            break;
                        case READY:
                            if (state.compareAndSet(State.READY, State.PUBLISHER_REQUESTED)) {
                                mainUpstream.request(1L);
                            } else {
                                retry = true;
                            }
                            break;
                        default:
                            break;
                    }
                } while (retry);
            }
        }

        @Override
        public void cancel() {
            mainUpstream.cancel();
            if (innerUpstream != null) {
                innerUpstream.cancel();
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

        private class InnerSubscriber implements MultiSubscriber<O>, ContextSupport {

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                innerUpstream = subscription;
                long n = demand.get();
                if (n > 0L) {
                    subscription.request(n);
                }
            }

            @Override
            public void onItem(O item) {
                innerOnItem(item);
            }

            @Override
            public void onFailure(Throwable failure) {
                innerOnFailure(failure);
            }

            @Override
            public void onCompletion() {
                innerOnCompletion();
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
