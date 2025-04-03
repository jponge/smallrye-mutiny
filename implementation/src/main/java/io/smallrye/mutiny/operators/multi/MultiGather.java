package io.smallrye.mutiny.operators.multi;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.tuples.Tuple2;

public class MultiGather<I, ACC, O> extends AbstractMultiOperator<I, O> {

    Supplier<ACC> initialAccumulatorSupplier;
    BiFunction<ACC, I, ACC> accumulator;
    Function<ACC, Optional<Tuple2<ACC, O>>> mapper;
    Function<ACC, Optional<O>> finalizer;

    public MultiGather(Multi<? extends I> upstream,
            Supplier<ACC> initialAccumulatorSupplier,
            BiFunction<ACC, I, ACC> accumulator,
            Function<ACC, Optional<Tuple2<ACC, O>>> mapper,
            Function<ACC, Optional<O>> finalizer) {
        super(upstream);
        this.initialAccumulatorSupplier = initialAccumulatorSupplier;
        this.accumulator = accumulator;
        this.mapper = mapper;
        this.finalizer = finalizer;
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> subscriber) {
        upstream.subscribe().withSubscriber(new MultiGatherProcessor(subscriber));
    }

    class MultiGatherProcessor extends MultiOperatorProcessor<I, O> {

        private ACC acc;
        private final AtomicLong demand = new AtomicLong();

        public MultiGatherProcessor(MultiSubscriber<? super O> downstream) {
            super(downstream);
            this.acc = initialAccumulatorSupplier.get();
            if (accumulator == null) {
                throw new IllegalArgumentException("The initial accumulator cannot be null");
            }
        }

        @Override
        public void request(long numberOfItems) {
            if (numberOfItems <= 0) {
                onFailure(new IllegalArgumentException("The number of items requested must be strictly positive"));
                return;
            }
            if (upstream != Subscriptions.CANCELLED) {
                Subscriptions.add(demand, numberOfItems);
                upstream.request(1L);
            }
        }

        @Override
        public void onItem(I item) {
            if (upstream == Subscriptions.CANCELLED) {
                return;
            }
            acc = accumulator.apply(acc, item);
            if (acc == null) {
                onFailure(new NullPointerException("The accumulator returned a null value"));
                return;
            }
            Optional<Tuple2<ACC, O>> mapping = mapper.apply(acc);
            if (mapping == null) {
                onFailure(new NullPointerException("The mapper returned a null value"));
                return;
            }
            if (mapping.isPresent()) {
                Tuple2<ACC, O> tuple = mapping.get();
                acc = tuple.getItem1();
                O value = tuple.getItem2();
                if (acc == null) {
                    onFailure(new NullPointerException("The mapper returned a null accumulator value"));
                    return;
                }
                if (value == null) {
                    onFailure(new NullPointerException("The mapper returned a null value to emit"));
                    return;
                }
                downstream.onItem(value);
                long remaining = demand.decrementAndGet();
                if (remaining > 0L) {
                    upstream.request(1L);
                }
            } else {
                upstream.request(1L);
            }
        }

        @Override
        public void onCompletion() {
            if (upstream == Subscriptions.CANCELLED) {
                return;
            }
            Optional<O> finalValue = finalizer.apply(acc);
            if (finalValue == null) {
                onFailure(new NullPointerException("The finalizer returned a null value"));
                return;
            }
            finalValue.ifPresent(o -> downstream.onItem(o));
            downstream.onCompletion();
        }
    }
}
