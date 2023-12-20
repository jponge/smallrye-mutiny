package io.smallrye.mutiny.operators.multi;

import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
        NEXT_PUBLISHER_REQUESTED,
        NEXT_PUBLISHER_INCOMING,
        EMITTING,
        DONE,
    }

    private static class MainSubscriber<I, O> implements MultiSubscriber<I>, Flow.Subscription, ContextSupport {

        private final Function<? super I, ? extends Publisher<? extends O>> mapper;
        private final boolean postponeFailurePropagation;

        private final MultiSubscriber<? super O> downstream;
        private volatile Flow.Subscription upstream;
        private final InnerSubscriber innerSubscriber = new InnerSubscriber();

        private volatile State state = State.INIT;
        private volatile Throwable failure;
        private volatile boolean mainCompleted;
        private volatile boolean innerCompleted;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicInteger terminationWorkInProgress = new AtomicInteger();

        private static final AtomicReferenceFieldUpdater<MultiConcatMapOp.MainSubscriber, Flow.Subscription> UPSTREAM_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(MultiConcatMapOp.MainSubscriber.class, Flow.Subscription.class, "upstream");
        private static final AtomicReferenceFieldUpdater<MultiConcatMapOp.MainSubscriber, State> STATE_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(MultiConcatMapOp.MainSubscriber.class, State.class, "state");

        private MainSubscriber(Function<? super I, ? extends Publisher<? extends O>> mapper, boolean postponeFailurePropagation,
                MultiSubscriber<? super O> downstream) {
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (UPSTREAM_UPDATER.compareAndSet(this, null, subscription)) {
                downstream.onSubscribe(this);
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onItem(I item) {
            if (STATE_UPDATER.compareAndSet(this, State.NEXT_PUBLISHER_REQUESTED, State.NEXT_PUBLISHER_INCOMING)) {
                try {
                    Publisher<? extends O> publisher = mapper.apply(item);
                    if (publisher == null) {
                        throw new NullPointerException("The mapper produced a null publisher");
                    }
                    publisher.subscribe(innerSubscriber);
                } catch (Throwable err) {
                    state = State.DONE;
                    upstream.cancel();
                    downstream.onFailure(addFailure(err));
                }
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (STATE_UPDATER.getAndSet(this, State.DONE) != State.DONE) {
                downstream.onFailure(addFailure(failure));
            }
        }

        @Override
        public void onCompletion() {
            if (state == State.DONE) {
                return;
            }
            mainCompleted = true;
            handlePossiblyTerminalEvent();
        }

        @Override
        public void request(long n) {
            if (state == State.DONE) {
                return;
            }
            if (n <= 0 && STATE_UPDATER.getAndSet(this, State.DONE) != State.DONE) {
                downstream.onFailure(Subscriptions.getInvalidRequestException());
                return;
            }
            Subscriptions.add(demand, n);
            if (state == State.EMITTING) {
                innerSubscriber.currentUpstream.request(n);
            } else if (STATE_UPDATER.compareAndSet(this, State.INIT, State.NEXT_PUBLISHER_REQUESTED)) {
                upstream.request(1L);
            }
        }

        @Override
        public void cancel() {
            State previousState = STATE_UPDATER.getAndSet(this, State.DONE);
            if (previousState != State.DONE) {
                upstream.cancel();
                if (previousState == State.EMITTING && innerSubscriber.currentUpstream != null) {
                    innerSubscriber.currentUpstream.cancel();
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

        private void handlePossiblyTerminalEvent() {
            if (terminationWorkInProgress.getAndIncrement() > 0) {
                return;
            }
            do {
                State currentState = state;
                boolean mainHasCompleted = mainCompleted;
                boolean innerHasCompleted = innerCompleted;
                if (mainHasCompleted && (innerHasCompleted
                        || currentState == State.NEXT_PUBLISHER_REQUESTED
                        || currentState == State.INIT)) {
                    state = State.DONE;
                    if (failure == null) {
                        downstream.onCompletion();
                    } else {
                        downstream.onFailure(failure);
                    }
                    return;
                } else if (innerHasCompleted) {
                    innerCompleted = false;
                    if (demand.get() > 0L) {
                        state = State.NEXT_PUBLISHER_REQUESTED;
                        upstream.request(1L);
                    } else {
                        state = State.INIT;
                    }
                }
            } while (terminationWorkInProgress.decrementAndGet() > 0);
        }

        private class InnerSubscriber implements MultiSubscriber<O>, ContextSupport {

            private Flow.Subscription currentUpstream;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (STATE_UPDATER.compareAndSet(MainSubscriber.this, State.NEXT_PUBLISHER_INCOMING, State.EMITTING)) {
                    currentUpstream = subscription;
                    long pending = demand.get();
                    if (pending > 0L) {
                        currentUpstream.request(pending);
                    }
                }
            }

            @Override
            public void onItem(O item) {
                if (state == State.DONE) {
                    return;
                }
                demand.decrementAndGet(); // TODO detect overflow?
                downstream.onItem(item);
            }

            @Override
            public void onFailure(Throwable failure) {
                if (state == State.DONE) {
                    return;
                }
                Throwable err = addFailure(failure);
                if (postponeFailurePropagation) {
                    innerCompleted = true; // TODO ensure we reset on continuation
                    handlePossiblyTerminalEvent();
                } else {
                    state = State.DONE;
                    upstream.cancel();
                    downstream.onFailure(err);
                }
            }

            @Override
            public void onCompletion() {
                if (state == State.DONE) {
                    return;
                }
                innerCompleted = true;
                handlePossiblyTerminalEvent();
            }

            @Override
            public Context context() {
                return MainSubscriber.this.context();
            }
        }
    }
}
