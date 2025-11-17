package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

import java.util.Queue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public final class SimplerMultiFlatMapOp<I, O> extends AbstractMultiOperator<I, O> {
    private final Function<? super I, ? extends Flow.Publisher<? extends O>> mapper;

    private final boolean postponeFailurePropagation;
    private final int maxConcurrency;
    private final int prefetch;

    public SimplerMultiFlatMapOp(Multi<? extends I> upstream,
                                 Function<? super I, ? extends Flow.Publisher<? extends O>> mapper,
                                 boolean postponeFailurePropagation,
                                 int maxConcurrency,
                                 int prefetch) {
        super(upstream);
        this.mapper = ParameterValidation.nonNull(mapper, "mapper");
        this.postponeFailurePropagation = postponeFailurePropagation;
        this.maxConcurrency = ParameterValidation.positive(maxConcurrency, "maxConcurrency");
        this.prefetch = ParameterValidation.positive(prefetch, "prefetch");
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("The subscriber must not be `null`");
        }
        MainSubscriber<I, O> mainSubscriber = new MainSubscriber<>(
                subscriber,
                mapper,
                postponeFailurePropagation,
                maxConcurrency,
                prefetch);
        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, mainSubscriber));
    }

    static class MainSubscriber<I, O> implements MultiSubscriber<I>, Subscription, ContextSupport {

        final MultiSubscriber<? super O> downstream;
        final Function<? super I, ? extends Flow.Publisher<? extends O>> mapper;
        final boolean postponeFailurePropagation;
        final int maxConcurrency;
        final int prefetch;
        final int replenishThreshold;

        MainSubscriber(MultiSubscriber<? super O> downstream, Function<? super I, ? extends Flow.Publisher<? extends O>> mapper,
                       boolean postponeFailurePropagation,
                       int maxConcurrency,
                       int prefetch) {
            this.downstream = downstream;
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
            this.maxConcurrency = maxConcurrency;
            this.prefetch = prefetch;
            this.replenishThreshold = Subscriptions.unboundedOrLimit(prefetch); // TODO not sure this is a good idea, fast producers might overflow if we refill before reaching the prefetch value
        }

        static final Subscription CANCELLED = Subscriptions.empty();
        static final Subscription COMPLETED = Subscriptions.empty();

        final AtomicReference<Subscription> mainUpstream = new AtomicReference<>();
        final AtomicReference<Throwable> failures = new AtomicReference<>();
        final Queue<O> itemsQueue = Queues.createMpscQueue();
        final AtomicInteger wip = new AtomicInteger();
        final AtomicLong demand = new AtomicLong();
        final CopyOnWriteArraySet<InnerSubscriber<I, O>> innerSubscribers = new CopyOnWriteArraySet<>();

        // TODO make sure collections get eventually cleared to prevent memory leaks

        @Override
        public void onSubscribe(Subscription subscription) {
            if (mainUpstream.compareAndSet(null, subscription)) {
                downstream.onSubscribe(this);
                subscription.request(Subscriptions.unboundedOrRequests(maxConcurrency));
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onItem(I item) {
            if (cancelled()) {
                return;
            }
            try {
                Flow.Publisher<? extends O> publisher = mapper.apply(item);
                if (publisher == null) {
                    throw new NullPointerException(ParameterValidation.MAPPER_RETURNED_NULL);
                }
                InnerSubscriber<I, O> inner = new InnerSubscriber<>(this);
                innerSubscribers.add(inner);
                publisher.subscribe(inner);
            } catch (Throwable err) {
                Subscriptions.addFailure(failures, err);
                cancel();
                // TODO handle terminal cases (onFailure vs onCompletion)
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (cancelled()) {
                return;
            }
        }

        @Override
        public void onCompletion() {
            if (cancelled()) {
                return;
            }
            mainUpstream.set(COMPLETED);
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                Subscriptions.add(demand, n);
                drain();
            } else {
                downstream.onFailure(Subscriptions.getInvalidRequestException());
            }
        }

        @Override
        public void cancel() {
            Subscription sub = mainUpstream.getAndSet(CANCELLED);
            if (sub != CANCELLED && sub != null) {
                sub.cancel();
                innerSubscribers.forEach(InnerSubscriber::cancel);
                innerSubscribers.clear();
                itemsQueue.clear();
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

        boolean cancelled() {
            return mainUpstream.get() == CANCELLED;
        }

        boolean completed() {
            return mainUpstream.get() == COMPLETED;
        }

        void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            drainLoop();
        }

        void drainLoop() {
            do {

                long emitted = 0L;
                long outstandingDemand = demand.get();
                while (emitted < outstandingDemand) {

                    if (cancelled()) {
                        return;
                    }

                    O item = itemsQueue.poll();
                    if (item == null) {
                        break;
                    }
                    downstream.onItem(item);
                    emitted++;
                }


                if (outstandingDemand != Long.MAX_VALUE) {
                    demand.addAndGet(-emitted);
                }

                int numberOfInner = 0;
                for (InnerSubscriber<I, O> inner : innerSubscribers) {
                    numberOfInner++;
                    if (inner.emittedOnCurrentBatch >= prefetch) {
                        inner.emittedOnCurrentBatch = 0L;
                        inner.request(prefetch);
                    }
                }
                if (demand.get() > 0 && numberOfInner < maxConcurrency && !completed()) {
                    mainUpstream.get().request(maxConcurrency - numberOfInner);
                }

            } while (wip.decrementAndGet() > 0);
        }

        void innerOnItem(O item) {
            if (cancelled()) {
                return;
            }
            if (!itemsQueue.add(item)) {
                // TODO handle possible failure
            }
            drain();
        }

        void innerOnFailure(InnerSubscriber<I, O> inner, Throwable err) {
            if (cancelled()) {
                return;
            }
            Subscriptions.addFailure(failures, err);
            innerSubscribers.remove(inner);
            if (postponeFailurePropagation) {
                drain();
            } else {
                cancel();
                downstream.onFailure(failures.get());
            }
        }

        public void innerOnCompletion(InnerSubscriber<I, O> inner) {
            if (cancelled()) {
                return;
            }
            innerSubscribers.remove(inner);
//            if (!completed()) {
//                mainUpstream.get().request(1L);
//            }
            drain();
        }
    }

    static class InnerSubscriber<I, O> implements Subscription, MultiSubscriber<O>, ContextSupport {

        final MainSubscriber<I, O> parent;

        InnerSubscriber(MainSubscriber<I, O> parent) {
            this.parent = parent;
        }

        final AtomicReference<Subscription> upstream = new AtomicReference<>();
        long emittedOnCurrentBatch;

        @Override
        public void onSubscribe(Subscription subscription) {
            if (upstream.compareAndSet(null, subscription)) {
                subscription.request(parent.prefetch);
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onItem(O item) {
            emittedOnCurrentBatch++;
            parent.innerOnItem(item);
        }

        @Override
        public void onFailure(Throwable failure) {
            upstream.set(MainSubscriber.CANCELLED);
            parent.innerOnFailure(this, failure);
        }

        @Override
        public void onCompletion() {
            upstream.set(MainSubscriber.COMPLETED);
            parent.innerOnCompletion(this);
        }

        @Override
        public void request(long n) {
            upstream.get().request(n);
        }

        @Override
        public void cancel() {
            Subscription sub = upstream.getAndSet(MainSubscriber.CANCELLED);
            if (sub != null && sub != MainSubscriber.CANCELLED) {
                sub.cancel();
            }
        }

        @Override
        public Context context() {
            return parent.context();
        }
    }

    // TODO this is a scratchpad
    public static void main(String[] args) {

        Multi<Integer> multi = Multi.createFrom().range(1, 10).log();
        SimplerMultiFlatMapOp<Integer, Integer> flatMap = new SimplerMultiFlatMapOp<>(
                multi,
                n -> Multi.createFrom().items(n, n * 10, n * 100),
                false,
                1,
                2);

        AssertSubscriber<Integer> sub = flatMap.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(3);
        sub.request(2);
        System.out.println(sub.getItems());
    }
}
