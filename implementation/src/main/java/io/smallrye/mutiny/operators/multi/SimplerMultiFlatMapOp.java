package io.smallrye.mutiny.operators.multi;

import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public final class SimplerMultiFlatMapOp<I, O> extends AbstractMultiOperator<I, O> {
    private final Function<? super I, ? extends Flow.Publisher<? extends O>> mapper;

    private final boolean postponeFailurePropagation; // TODO Docs: "Instructs the flatMap operation to consume all the streams returned by the mapper before propagating a failure if any of the stream has produced a failure."
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

        MainSubscriber(MultiSubscriber<? super O> downstream, Function<? super I, ? extends Flow.Publisher<? extends O>> mapper,
                       boolean postponeFailurePropagation,
                       int maxConcurrency,
                       int prefetch) {
            this.downstream = downstream;
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
            this.maxConcurrency = maxConcurrency;
            this.prefetch = prefetch;
            this.itemsQueue = Queues.createMpscQueue(Math.max(prefetch, 256));
        }

        static final Subscription CANCELLED = Subscriptions.empty();
        static final Subscription COMPLETED = Subscriptions.empty();

        final AtomicReference<Subscription> mainUpstream = new AtomicReference<>();
        final AtomicReference<Throwable> failures = new AtomicReference<>();
        final Queue<O> itemsQueue;
        final AtomicInteger wip = new AtomicInteger();
        final AtomicLong demand = new AtomicLong();
        final CopyOnWriteArraySet<InnerSubscriber<I, O>> innerSubscribers = new CopyOnWriteArraySet<>();
        final AtomicBoolean initialRequestHasBeenMade = new AtomicBoolean();
        final AtomicInteger refillRequests = new AtomicInteger();

        // TODO make sure collections get eventually cleared to prevent memory leaks

        @Override
        public void onSubscribe(Subscription subscription) {
            if (mainUpstream.compareAndSet(null, subscription)) {
                downstream.onSubscribe(this);
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
                terminate();
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (cancelled()) {
                return;
            }
            cancel();
            Subscriptions.addFailure(failures, failure);
            terminate();
        }

        @Override
        public void onCompletion() {
            if (cancelled()) {
                return;
            }
            mainUpstream.set(COMPLETED);
            drain();
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                if (cancelled()) {
                    return;
                }
                Subscriptions.add(demand, n);
                if (initialRequestHasBeenMade.compareAndSet(false, true)) {
                    mainUpstream.get().request(Subscriptions.unboundedOrRequests(maxConcurrency));
                }
                drain();
            } else {
                cancel();
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

        boolean failed() {
            return failures.get() != null;
        }

        void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            drainLoop();
        }

        void drainLoop() {
            do {

                // Cancellation check
                if (cancelled()) {
                    return;
                }

                // Check for failure
                if (failed() && !postponeFailurePropagation) {
                    cancel();
                    terminate();
                    return;
                }

                // Inner loop over the queue
                long emitted = 0L;
                long outstandingDemand = demand.get();
                while (emitted < outstandingDemand) {

                    // Cancellation check
                    if (cancelled()) {
                        return;
                    }

                    // Check for failure
                    if (failed() && !postponeFailurePropagation) {
                        cancel();
                        terminate();
                        return;
                    }

                    // Item dispatch
                    O item = itemsQueue.poll();
                    if (item == null) {
                        break;
                    }
                    downstream.onItem(item);
                    emitted++;
                }

                // Demand tracking when bounded
                if (outstandingDemand != Long.MAX_VALUE) {
                    demand.addAndGet(-emitted);
                }

                // Replenish inners
                int numberOfInner = 0;
                for (InnerSubscriber<I, O> inner : innerSubscribers) {
                    if (cancelled()) {
                        return;
                    }
                    numberOfInner++;
                    if (inner.emittedOnCurrentBatch >= prefetch) {
                        inner.emittedOnCurrentBatch = 0L;
                        inner.request(prefetch);
                    }
                }

                // Replenish main
                if (cancelled()) {
                    return;
                }
                int refillCount = refillRequests.getAndSet(0);
                if (refillCount > 0 && !completed() && !cancelled()) {
                    mainUpstream.get().request(Math.min(maxConcurrency, refillCount));
                }

                // Check for completion
                if (numberOfInner == 0 && itemsQueue.isEmpty() && completed() && !cancelled()) {
                    mainUpstream.set(CANCELLED);
                    terminate();
                    return;
                }

            } while (wip.decrementAndGet() > 0);
        }

        void terminate() {
            Throwable collectedFailures = failures.getAndSet(null);
            if (collectedFailures == null) {
                downstream.onComplete();
            } else {
                downstream.onFailure(collectedFailures);
            }
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
                refillRequests.incrementAndGet();
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
            refillRequests.incrementAndGet();
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
            if (sub != MainSubscriber.CANCELLED && sub != null) {
                sub.cancel();
            }
        }

        @Override
        public Context context() {
            return parent.context();
        }
    }

    // TODO this is a scratchpad

    static class Main_1 {
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
            System.out.println(sub.getItems());

            sub.request(2);
            System.out.println(sub.getItems());

            //sub.request(Long.MAX_VALUE);
            //System.out.println(sub.getItems());
        }
    }

    static class Main_2 {
        public static void main(String[] args) {

            Random random = new Random();
            Multi<Integer> multi = Multi.createFrom().range(1, 10).log();
            SimplerMultiFlatMapOp<Integer, Integer> flatMap = new SimplerMultiFlatMapOp<>(
                    multi,
                    n -> Multi.createFrom().emitter(em -> {
                        new Thread(() -> {
                            try {
                                Thread.sleep(random.nextInt(500));
                                em.emit(n);
                                Thread.sleep(random.nextInt(500));
                                em.emit(n * 10);
                                Thread.sleep(random.nextInt(500));
                                em.emit(n * 100);
                                em.complete();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }).start();
                    }),
                    false,
                    2,
                    16);

            AssertSubscriber<Integer> sub = flatMap.subscribe().withSubscriber(AssertSubscriber.create());

            sub.awaitNextItems(3);
            System.out.println(sub.getItems());

            sub.awaitNextItems(2);
            System.out.println(sub.getItems());

            sub.awaitNextItems(4);
            System.out.println(sub.getItems());

            sub.request(Long.MAX_VALUE);
            sub.awaitCompletion();

            System.out.println(sub.getItems());
            System.out.println(sub.getItems().size());
        }
    }

    static class Main_3 {
        public static void main(String[] args) {

            Multi<Integer> multi = Multi.createFrom().range(1, 10).log();
            SimplerMultiFlatMapOp<Integer, String> flatMap = new SimplerMultiFlatMapOp<>(
                    multi,
                    n -> Multi.createFrom().emitter(emitter -> {
                        emitter.emit(n + ": foo");
                        emitter.emit(n + ": bar");
                        emitter.emit(n + ": baz");
                        emitter.fail(new IOException(n + " // boom"));
                    }),
                    true,
                    2,
                    2);

            AssertSubscriber<String> sub = flatMap.subscribe().withSubscriber(AssertSubscriber.create());

            System.out.println("req(2)");
            sub.request(2);
            System.out.println(sub.getItems());
            System.out.println("!!! " + sub.getFailure());

            System.out.println("req(2)");
            sub.request(2);
            System.out.println(sub.getItems());
            System.out.println("!!! " + sub.getFailure());

            System.out.println("req(max)");
            sub.request(Long.MAX_VALUE);
            System.out.println(sub.getItems());
            System.out.println("!!! " + sub.getFailure());

            sub.assertFailedWith(CompositeException.class);
        }
    }

    static class Main_3_OldOperatorGoesWrong {

        // Our friends at Reactor have it right, our current flatMap... not :-)
        // What we want is to consume all the current and future streams, then eventually fail

//        Flux<String> flux = Flux.range(1, 9)
//                .flatMapDelayError(n -> Flux.create(sink -> {
//                    sink.next(n + ": foo");
//                    sink.next(n + ": bar");
//                    sink.next(n + ": baz");
//                    sink.error(new IOException(n + ": boom"));
//                }), 2, 2);
//
//        flux.subscribe(
//        item -> System.out.println(">>> " + item),
//        Throwable::printStackTrace);

        public static void main(String[] args) {

            Multi<Integer> multi = Multi.createFrom().range(1, 10).log();
            MultiFlatMapOp<Integer, String> flatMap = new MultiFlatMapOp<>(
                    multi,
                    n -> Multi.createFrom().emitter(emitter -> {
                        emitter.emit(n + ": foo");
                        emitter.emit(n + ": bar");
                        emitter.emit(n + ": baz");
                        emitter.fail(new IOException(n + " // boom"));
                    }),
                    true,
                    2,
                    2);

            AssertSubscriber<String> sub = flatMap.subscribe().withSubscriber(AssertSubscriber.create());

            System.out.println("req(2)");
            sub.request(2);
            System.out.println(sub.getItems());
            System.out.println("!!! " + sub.getFailure());

            System.out.println("req(2)");
            sub.request(2);
            System.out.println(sub.getItems());
            System.out.println("!!! " + sub.getFailure());

            System.out.println("req(max)");
            sub.request(Long.MAX_VALUE);
            System.out.println(sub.getItems());
            System.out.println("!!! " + sub.getFailure());
        }
    }
}
