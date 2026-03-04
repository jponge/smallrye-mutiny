package io.smallrye.mutiny.operators.multi;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.*;
import java.util.function.Function;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public final class MultiFlatMapOp<I, O> extends AbstractMultiOperator<I, O> {
    private final Function<? super I, ? extends Flow.Publisher<? extends O>> mapper;

    private final boolean postponeFailurePropagation;
    private final int maxConcurrency;
    private final int requests;

    public MultiFlatMapOp(Multi<? extends I> upstream,
            Function<? super I, ? extends Flow.Publisher<? extends O>> mapper,
            boolean postponeFailurePropagation,
            int maxConcurrency,
            int requests) {
        super(upstream);
        this.mapper = ParameterValidation.nonNull(mapper, "mapper");
        this.postponeFailurePropagation = postponeFailurePropagation;
        this.maxConcurrency = ParameterValidation.positive(maxConcurrency, "maxConcurrency");
        this.requests = ParameterValidation.positive(requests, "requests");
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("The subscriber must not be `null`");
        }
        FlatMapMainSubscriber<I, O> sub = new FlatMapMainSubscriber<>(subscriber,
                mapper,
                postponeFailurePropagation,
                maxConcurrency,
                requests);

        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, sub));
    }

    public static final class FlatMapMainSubscriber<I, O>
            implements MultiSubscriber<I>, Subscription, ContextSupport {

        final boolean delayError;
        final int maxConcurrency;
        final int requests;
        final Function<? super I, ? extends Flow.Publisher<? extends O>> mapper;
        final MultiSubscriber<? super O> downstream;

        final Queue<O> queue;

        final AtomicReference<Throwable> failures = new AtomicReference<>();

        volatile boolean done;
        volatile boolean cancelled;

        volatile Subscription upstream = null;
        @SuppressWarnings("rawtypes")
        private static final AtomicReferenceFieldUpdater<FlatMapMainSubscriber, Subscription> UPSTREAM_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(FlatMapMainSubscriber.class, Subscription.class, "upstream");

        AtomicLong requested = new AtomicLong();

        AtomicInteger wip = new AtomicInteger();

        private final ArrayList<FlatMapInner<O>> inners = new ArrayList<>();
        private volatile boolean terminated;
        final AtomicInteger activeCount = new AtomicInteger();
        final AtomicLong pendingUpstreamRequests = new AtomicLong();

        public FlatMapMainSubscriber(MultiSubscriber<? super O> downstream,
                Function<? super I, ? extends Flow.Publisher<? extends O>> mapper,
                boolean delayError,
                int concurrency,
                int requests) {
            this.downstream = downstream;
            this.mapper = mapper;
            this.delayError = delayError;
            this.maxConcurrency = concurrency;
            this.requests = requests;
            this.queue = Queues.createMpscQueue();
        }

        boolean addInner(FlatMapInner<O> inner) {
            synchronized (inners) {
                if (terminated) {
                    return false;
                }
                inners.add(inner);
            }
            activeCount.incrementAndGet();
            return true;
        }

        void removeInner(FlatMapInner<O> inner) {
            synchronized (inners) {
                inners.remove(inner);
            }
            activeCount.decrementAndGet();
        }

        void cancelAllInners() {
            List<FlatMapInner<O>> snapshot;
            synchronized (inners) {
                terminated = true;
                snapshot = new ArrayList<>(inners);
                inners.clear();
            }
            activeCount.set(0);
            for (FlatMapInner<O> inner : snapshot) {
                inner.cancelSubscription();
            }
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                Subscriptions.add(requested, n);
                drain();
            } else {
                downstream.onFailure(new IllegalArgumentException("Invalid requests, must be greater than 0"));
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;

                if (wip.getAndIncrement() == 0) {
                    cancelUpstream();
                }
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (UPSTREAM_UPDATER.compareAndSet(this, null, s)) {
                downstream.onSubscribe(this);
                s.request(Subscriptions.unboundedOrRequests(maxConcurrency));
            }
        }

        @Override
        public void onItem(I item) {
            if (done) {
                return;
            }

            Flow.Publisher<? extends O> p;

            try {
                p = mapper.apply(item);
                if (p == null) {
                    throw new NullPointerException(ParameterValidation.MAPPER_RETURNED_NULL);
                }
            } catch (Throwable e) {
                cancelled = true;
                done = true;
                Subscriptions.addFailure(failures, e);
                cancelUpstream();
                handleTerminationIfDone();
                return;
            }

            FlatMapInner<O> inner = new FlatMapInner<>(this, requests);
            if (addInner(inner)) {
                p.subscribe(inner);
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (done) {
                Infrastructure.handleDroppedException(failure);
                return;
            }
            Subscriptions.addFailure(failures, failure);
            done = true;
            if (!delayError) {
                cancelAllInners();
            }
            drain();
        }

        @Override
        public void onCompletion() {
            if (done) {
                return;
            }

            done = true;
            drain();
        }

        void tryEmit(O item) {
            if (wip.compareAndSet(0, 1)) {
                long req = requested.get();
                if (req != 0L && queue.isEmpty()) {
                    downstream.onNext(item);

                    if (req != Long.MAX_VALUE) {
                        requested.decrementAndGet();
                    }
                } else {
                    queue.offer(item);
                }
                if (wip.decrementAndGet() == 0) {
                    return;
                }

                drainLoop();
            } else {
                queue.offer(item);
                drain();
            }
        }

        void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            drainLoop();
        }

        void drainLoop() {
            int missed = 1;

            for (;;) {
                if (ifDoneOrCancelled()) {
                    return;
                }

                long r = requested.get();
                long e = 0L;

                while (e != r) {
                    if (cancelled) {
                        cancelUpstream();
                        return;
                    }
                    O v = queue.poll();
                    if (v == null) {
                        break;
                    }
                    downstream.onNext(v);
                    e++;
                }

                if (e != 0L && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }

                if (queue.isEmpty()) {
                    long replenish = pendingUpstreamRequests.getAndSet(0);
                    if (replenish != 0L && !done && !cancelled) {
                        upstream.request(replenish);
                    }
                }

                if (ifDoneOrCancelled()) {
                    return;
                }

                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        private void cancelUpstream() {
            Subscription subscription = UPSTREAM_UPDATER.getAndSet(this, Subscriptions.CANCELLED);
            if (subscription != null) {
                subscription.cancel();
            }
            cancelAllInners();
            queue.clear();
        }

        boolean ifDoneOrCancelled() {
            if (cancelled) {
                cancelUpstream();
                return true;
            }

            return handleTerminationIfDone();
        }

        private boolean handleTerminationIfDone() {
            boolean wasDone = done;
            boolean noActive = activeCount.get() == 0;
            boolean queueEmpty = queue.isEmpty();
            if (delayError) {
                if (wasDone && noActive && queueEmpty) {
                    Throwable e = failures.get();
                    if (e != null && e != Subscriptions.TERMINATED) {
                        Throwable throwable = failures.getAndSet(Subscriptions.TERMINATED);
                        downstream.onFailure(throwable);
                    } else {
                        downstream.onCompletion();
                    }
                    return true;
                }
            } else {
                if (wasDone) {
                    Throwable e = failures.get();
                    if (e != null && e != Subscriptions.TERMINATED) {
                        Throwable throwable = failures.getAndSet(Subscriptions.TERMINATED);
                        cancelAllInners();
                        downstream.onFailure(throwable);
                        return true;
                    } else if (noActive && queueEmpty) {
                        downstream.onCompletion();
                        return true;
                    }
                }
            }
            return false;
        }

        void innerComplete(FlatMapInner<O> inner) {
            removeInner(inner);
            if (!done && !cancelled) {
                pendingUpstreamRequests.incrementAndGet();
            }
            drain();
        }

        void innerError(FlatMapInner<O> inner, Throwable fail) {
            removeInner(inner);
            if (Subscriptions.addFailure(failures, fail)) {
                if (!delayError) {
                    done = true;
                    cancelUpstream();
                } else if (!done && !cancelled) {
                    pendingUpstreamRequests.incrementAndGet();
                }
                drain();
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

    static final class FlatMapInner<O> implements MultiSubscriber<O>, ContextSupport {

        final FlatMapMainSubscriber<?, O> parent;

        final int prefetch;

        final int replenishMark;

        volatile Subscription subscription = null;
        @SuppressWarnings("rawtypes")
        private static final AtomicReferenceFieldUpdater<FlatMapInner, Subscription> SUBSCRIPTION_UPDATER = AtomicReferenceFieldUpdater
                .newUpdater(FlatMapInner.class, Subscription.class, "subscription");

        int produced;

        FlatMapInner(FlatMapMainSubscriber<?, O> parent, int prefetch) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.replenishMark = Subscriptions.unboundedOrLimit(prefetch);
        }

        @Override
        public void onSubscribe(Subscription s) {
            Objects.requireNonNull(s);
            if (SUBSCRIPTION_UPDATER.compareAndSet(this, null, s)) {
                s.request(Subscriptions.unboundedOrRequests(prefetch));
            }
        }

        @Override
        public void onItem(O item) {
            int p = produced + 1;
            if (p >= replenishMark) {
                produced = 0;
                subscription.request(p);
            } else {
                produced = p;
            }
            parent.tryEmit(item);
        }

        @Override
        public void onFailure(Throwable failure) {
            Objects.requireNonNull(failure);
            parent.innerError(this, failure);
        }

        @Override
        public void onCompletion() {
            parent.innerComplete(this);
        }

        void cancelSubscription() {
            Subscription last = SUBSCRIPTION_UPDATER.getAndSet(this, Subscriptions.CANCELLED);
            if (last != null) {
                last.cancel();
            }
        }

        @Override
        public Context context() {
            return parent.context();
        }
    }
}
