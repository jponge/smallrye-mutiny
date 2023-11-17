package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.subscription.SwitchableSubscriptionSubscriber;

import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

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
        //        ConcatMapMainSubscriber<I, O> sub = new ConcatMapMainSubscriber<>(subscriber,
        //                mapper,
        //                postponeFailurePropagation);
        //
        //        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, sub));
        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, new ConcatMapSubscriber(subscriber)));
    }

    private enum State {
        INIT,
        WAITING_NEXT_PUBLISHER,
        WAITING_NEXT_SUBSCRIPTION,
        EMITTING,
        CANCELLED,
    }

    final class ConcatMapSubscriber implements MultiSubscriber<I>, Subscription {

        private final MultiSubscriber<? super O> downstream;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
        private Subscription upstream;
        private Subscription currentUpstream;
        private boolean upstreamHasCompleted = false;
        private Throwable failure;

        ConcatMapSubscriber(MultiSubscriber<? super O> downstream) {
            this.downstream = downstream;
        }

        private final MultiSubscriber<O> innerSubscriber = new MultiSubscriber<>() {

            @Override
            public void onSubscribe(Subscription subscription) {
                if (state.get() == State.CANCELLED) {
                    return;
                }
                if (state.get() != State.WAITING_NEXT_SUBSCRIPTION) {
                    // TODO protocol failure
                    System.out.println("woops");
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
                ConcatMapSubscriber.this.onFailure(failure);
            }

            @Override
            public void onCompletion() {
                if (state.get() == State.CANCELLED) {
                    return;
                }
                if (!upstreamHasCompleted) {
                    state.set(State.WAITING_NEXT_PUBLISHER);
                    upstream.request(1L);
                } else {
                    completeOrFail();
                }
            }
        };

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
                    onFailure(err);
                }
            } else {
                // TODO protocol error
                System.out.println("woops");
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (state.get() == State.CANCELLED) {
                return;
            }
            if (this.failure != null) {
                if (this.failure instanceof CompositeException) {
                    this.failure = new CompositeException((CompositeException) this.failure, failure);
                } else {
                    this.failure = new CompositeException(this.failure, failure);
                }
            } else {
                this.failure = failure;
            }
            if (postponeFailurePropagation) {
                innerSubscriber.onCompletion();
            } else {
                completeOrFail();
            }
        }

        private void completeOrFail() {
            state.set(State.CANCELLED);
            if (failure != null) {
                downstream.onFailure(failure);
            } else {
                downstream.onComplete();
            }
        }

        @Override
        public void onCompletion() {
            if (state.get() == State.CANCELLED) {
                return;
            }
            upstreamHasCompleted = true;
            if (state.compareAndSet(State.WAITING_NEXT_PUBLISHER, State.CANCELLED)) {
                completeOrFail();
            }
        }

        @Override
        public void request(long n) {
            if (state.get() == State.CANCELLED) {
                return;
            }
            if (n <= 0) {
                state.set(State.CANCELLED);
                downstream.onFailure(Subscriptions.getInvalidRequestException());
            } else {
                Subscriptions.add(demand, n);
                if (state.compareAndSet(State.INIT, State.WAITING_NEXT_PUBLISHER)) {
                    upstream.request(1L);
                } else if (state.get() == State.EMITTING) {
                    currentUpstream.request(n);
                }
            }
        }

        @Override
        public void cancel() {
            if (state.getAndSet(State.CANCELLED) != State.CANCELLED) {
                if (upstream != null) {
                    upstream.cancel();
                }
                if (currentUpstream != null) {
                    currentUpstream.cancel();
                }
            }
        }
    }

    public static final class ConcatMapMainSubscriber<I, O> implements MultiSubscriber<I>, Subscription, ContextSupport {

        private static final int STATE_NEW = 0; // no request yet -- send first upstream request at this state
        private static final int STATE_READY = 1; // first upstream request done, ready to receive items
        private static final int STATE_EMITTING = 2; // received item from the upstream, subscribed to the inner
        private static final int STATE_OUTER_TERMINATED = 3; // outer terminated, waiting for the inner to terminate
        private static final int STATE_TERMINATED = 4; // inner and outer terminated
        private static final int STATE_CANCELLED = 5; // cancelled
        final AtomicInteger state = new AtomicInteger(STATE_NEW);

        final MultiSubscriber<? super O> downstream;
        final Function<? super I, ? extends Publisher<? extends O>> mapper;
        private final boolean delayError;

        final AtomicReference<Throwable> failures = new AtomicReference<>();

        volatile Subscription upstream = null;
        private static final AtomicReferenceFieldUpdater<ConcatMapMainSubscriber, Subscription> UPSTREAM_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(ConcatMapMainSubscriber.class, Subscription.class, "upstream");

        final ConcatMapInner<O> inner;

        private final AtomicBoolean deferredUpstreamRequest = new AtomicBoolean(false);

        ConcatMapMainSubscriber(
                MultiSubscriber<? super O> downstream,
                Function<? super I, ? extends Publisher<? extends O>> mapper,
                boolean delayError) {
            this.downstream = downstream;
            this.mapper = mapper;
            this.delayError = delayError;
            this.inner = new ConcatMapInner<>(this);
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                if (state.compareAndSet(STATE_NEW, STATE_READY)) {
                    upstream.request(1);
                }
                if (deferredUpstreamRequest.compareAndSet(true, false)) {
                    upstream.request(1);
                }
                inner.request(n);
                if (inner.requested() != 0L && deferredUpstreamRequest.compareAndSet(true, false)) {
                    upstream.request(1);
                }
            } else {
                downstream.onFailure(Subscriptions.getInvalidRequestException());
            }
        }

        @Override
        public void cancel() {
            while (true) {
                int state = this.state.get();
                if (state == STATE_CANCELLED) {
                    return;
                }
                if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                    if (state == STATE_OUTER_TERMINATED) {
                        inner.cancel();
                    } else {
                        inner.cancel();
                        upstream.cancel();
                    }
                    return;
                }
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (UPSTREAM_UPDATER.compareAndSet(this, null, s)) {
                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onItem(I item) {
            if (!state.compareAndSet(STATE_READY, STATE_EMITTING)) {
                return;
            }

            try {
                Publisher<? extends O> p = mapper.apply(item);
                if (p == null) {
                    throw new NullPointerException(ParameterValidation.MAPPER_RETURNED_NULL);
                }

                p.subscribe(inner);
            } catch (Throwable e) {
                if (postponeFailure(e, upstream)) {
                    innerComplete(0L);
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (postponeFailure(t, inner)) {
                onCompletion();
            }
        }

        @Override
        public void onCompletion() {
            while (true) {
                int state = this.state.get();
                if (state == STATE_NEW || state == STATE_READY) {
                    if (this.state.compareAndSet(state, STATE_TERMINATED)) {
                        terminateDownstream();
                        return;
                    }
                } else if (state == STATE_EMITTING) {
                    if (this.state.compareAndSet(state, STATE_OUTER_TERMINATED)) {
                        return;
                    }
                } else {
                    return;
                }
            }
        }

        public synchronized void tryEmit(O value) {
            switch (state.get()) {
                case STATE_EMITTING:
                case STATE_OUTER_TERMINATED:
                    downstream.onItem(value);
                    break;
                default:
                    break;
            }
        }

        public void innerComplete(long emitted) {
            if (this.state.compareAndSet(STATE_EMITTING, STATE_READY)) {
                // Inner completed but there are outstanding requests from inner,
                // Or the inner completed without producing any items
                // Request new item from upstream
                if (inner.requested() != 0L || emitted == 0) {
                    upstream.request(1);
                } else {
                    deferredUpstreamRequest.set(true);
                }
            } else if (this.state.compareAndSet(STATE_OUTER_TERMINATED, STATE_TERMINATED)) {
                terminateDownstream();
            }
        }

        public void innerFailure(Throwable e, long emitted) {
            if (postponeFailure(e, upstream)) {
                innerComplete(emitted);
            }
        }

        private boolean postponeFailure(Throwable e, Subscription subscription) {
            if (e == null) {
                return true;
            }

            Subscriptions.addFailure(failures, e);

            if (delayError) {
                return true;
            }

            while (true) {
                int state = this.state.get();
                if (state == STATE_CANCELLED || state == STATE_TERMINATED) {
                    return false;
                } else {
                    if (this.state.compareAndSet(state, STATE_TERMINATED)) {
                        subscription.cancel();
                        synchronized (this) {
                            downstream.onFailure(failures.get());
                        }
                        return false;
                    }
                }
            }
        }

        private void terminateDownstream() {
            Throwable ex = failures.get();
            if (ex != null) {
                downstream.onFailure(ex);
                return;
            }
            downstream.onCompletion();
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

    static final class ConcatMapInner<O> extends SwitchableSubscriptionSubscriber<O> {
        private final ConcatMapMainSubscriber<?, O> parent;

        long emitted;

        /**
         * Downstream passed as {@code null} to {@link SwitchableSubscriptionSubscriber} as accessors are not reachable.
         * Effective downstream is {@code parent}.
         *
         * @param parent parent as downstream
         */
        ConcatMapInner(ConcatMapMainSubscriber<?, O> parent) {
            super(null);
            this.parent = parent;
        }

        @Override
        public void onItem(O item) {
            emitted++;
            parent.tryEmit(item);
        }

        @Override
        public void onFailure(Throwable failure) {
            long p = emitted;

            if (p != 0L) {
                emitted = 0L;
                emitted(p);
            }

            parent.innerFailure(failure, p);
        }

        @Override
        public void onCompletion() {
            long p = emitted;

            if (p != 0L) {
                emitted = 0L;
                emitted(p);
            }

            parent.innerComplete(p);
        }

        @Override
        public Context context() {
            return parent.context();
        }
    }
}
