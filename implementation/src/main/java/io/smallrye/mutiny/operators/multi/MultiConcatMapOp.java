package io.smallrye.mutiny.operators.multi;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
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
        ConcatMapSubscriber<I, O> concatMapSubscriber = new ConcatMapSubscriber<>(mapper, postponeFailurePropagation,
                subscriber);
        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, concatMapSubscriber));
    }

    static class ConcatMapSubscriber<I, O> implements MultiSubscriber<I>, Subscription, ContextSupport {

        private enum State {
            INIT,
            WAITING_NEXT_PUBLISHER,
            WAITING_NEXT_SUBSCRIPTION,
            EMITTING,
            CANCELLED,
        }

        private final Function<? super I, ? extends Publisher<? extends O>> mapper;
        private final boolean postponeFailurePropagation;
        private final MultiSubscriber<? super O> downstream;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
        private Subscription upstream;
        private Subscription currentUpstream;
        private boolean upstreamHasCompleted = false;
        private Throwable failure;

        ConcatMapSubscriber(Function<? super I, ? extends Publisher<? extends O>> mapper, boolean postponeFailurePropagation,
                MultiSubscriber<? super O> downstream) {
            this.downstream = downstream;
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
        }

        @Override
        public Context context() {
            if (downstream instanceof ContextSupport) {
                return ((ContextSupport) downstream).context();
            } else {
                return Context.empty();
            }
        }

        private final MultiSubscriber<O> innerSubscriber = new InnerSubscriber();

        @Override
        public void onSubscribe(Subscription subscription) {
            if (upstream == null) {
                upstream = subscription;
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
            if (state.compareAndSet(State.WAITING_NEXT_PUBLISHER, State.WAITING_NEXT_SUBSCRIPTION)) {
                try {
                    Publisher<? extends O> publisher = mapper.apply(item);
                    if (publisher == null) {
                        throw new NullPointerException("The mapper produced a null publisher");
                    }
                    publisher.subscribe(innerSubscriber);
                } catch (Throwable err) {
                    upstream.cancel();
                    onFailure(err);
                }
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (state.getAndSet(State.CANCELLED) == State.CANCELLED) {
                return;
            }
            downstream.onFailure(addFailure(failure));
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
            if (state.get() == State.CANCELLED) {
                return;
            }
            upstreamHasCompleted = true;
            if (state.compareAndSet(State.WAITING_NEXT_PUBLISHER, State.CANCELLED)
                    || state.compareAndSet(State.INIT, State.CANCELLED)) {
                if (failure == null) {
                    downstream.onCompletion();
                } else {
                    downstream.onFailure(failure);
                }
            }
        }

        @Override
        public void request(long n) {
            State currentState = state.get();
            if (currentState == State.CANCELLED) {
                return;
            }
            if (n <= 0) {
                cancel();
                downstream.onFailure(Subscriptions.getInvalidRequestException());
            } else {
                Subscriptions.add(demand, n);
                if (state.compareAndSet(State.INIT, State.WAITING_NEXT_PUBLISHER)) {
                    upstream.request(1L);
                } else {
                    if (currentState == State.WAITING_NEXT_PUBLISHER) {
                        upstream.request(1L);
                    } else if (currentState == State.EMITTING) {
                        currentUpstream.request(n);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            State previousState = state.getAndSet(State.CANCELLED);
            if (previousState == State.CANCELLED) {
                return;
            }
            if (previousState == State.EMITTING) {
                currentUpstream.cancel();
                upstream.cancel();
            } else if (upstream != null) {
                upstream.cancel();
            }
        }

        class InnerSubscriber implements MultiSubscriber<O>, ContextSupport {

            @Override
            public void onSubscribe(Subscription subscription) {
                if (state.get() == State.CANCELLED) {
                    return;
                }
                currentUpstream = subscription;
                state.set(State.EMITTING);
                long pending = demand.get();
                if (pending > 0L) {
                    currentUpstream.request(pending);
                }
            }

            @Override
            public void onItem(O item) {
                if (state.get() == State.CANCELLED) {
                    return;
                }
                downstream.onItem(item);
                demand.decrementAndGet();
            }

            @Override
            public void onFailure(Throwable failure) {
                if (state.get() == State.CANCELLED) {
                    return;
                }
                Throwable err = addFailure(failure);
                if (postponeFailurePropagation) {
                    onCompletion();
                } else {
                    state.set(State.CANCELLED);
                    upstream.cancel();
                    downstream.onFailure(err);
                }
            }

            @Override
            public void onCompletion() {
                if (state.get() == State.CANCELLED) {
                    return;
                }
                if (!upstreamHasCompleted) {
                    state.set(State.WAITING_NEXT_PUBLISHER);
                    if (demand.get() > 0L) {
                        upstream.request(1L);
                    }
                } else {
                    state.set(State.CANCELLED);
                    if (failure != null) {
                        downstream.onFailure(failure);
                    } else {
                        downstream.onComplete();
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
