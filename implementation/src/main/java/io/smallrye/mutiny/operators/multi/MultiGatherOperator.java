package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiSubscriber;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.Subscriptions.CANCELLED;

public class MultiGatherOperator<IN, ACC, OUT> extends AbstractMultiOperator<IN, OUT> {

    private final Supplier<ACC> accumulatorSupplier;
    private final BiFunction<ACC, IN, ACC> accumulator;
    private final Predicate<ACC> shouldEmit;
    private final Function<ACC, ACC> splitter;
    private final BiFunction<? super ACC, Boolean, OUT> outputTransformer;

    public MultiGatherOperator(Multi<? extends IN> upstream,
                               Supplier<ACC> accumulatorSupplier,
                               BiFunction<ACC, IN, ACC> accumulator,
                               Predicate<ACC> shouldEmit,
                               Function<ACC, ACC> splitter,
                               BiFunction<? super ACC, Boolean, OUT> outputTransformer) {
        super(upstream);
        this.accumulatorSupplier = nonNull(accumulatorSupplier, "accumulatorSupplier");
        this.accumulator = nonNull(accumulator, "accumulator");
        this.shouldEmit = nonNull(shouldEmit, "shouldEmit");
        this.splitter = nonNull(splitter, "splitter");
        this.outputTransformer = nonNull(outputTransformer, "outputTransformer");
    }

    @Override
    public void subscribe(MultiSubscriber<? super OUT> subscriber) {
        upstream().subscribe().withSubscriber(
                new GatherProcessor<>(subscriber, accumulatorSupplier, accumulator, shouldEmit, splitter, outputTransformer));
    }

    static final class GatherProcessor<IN, ACC, OUT> extends MultiOperatorProcessor<IN, OUT> {

        private final BiFunction<ACC, IN, ACC> accumulator;
        private final Predicate<ACC> shouldEmit;
        private final Function<ACC, ACC> splitter;
        private final BiFunction<? super ACC, Boolean, OUT> outputTransformer;

        private ACC accumulated;
        private final AtomicLong pendingRequests = new AtomicLong();

        public GatherProcessor(
                MultiSubscriber<? super OUT> downstream,
                Supplier<ACC> accumulatorSupplier,
                BiFunction<ACC, IN, ACC> accumulator,
                Predicate<ACC> shouldEmit,
                Function<ACC, ACC> splitter,
                BiFunction<? super ACC, Boolean, OUT> outputTransformer) {
            super(downstream);
            this.accumulator = accumulator;
            this.shouldEmit = shouldEmit;
            this.splitter = splitter;
            this.outputTransformer = outputTransformer;
            this.accumulated = nonNull(accumulatorSupplier.get(), "accumulated");
        }

        @Override
        public void request(long numberOfItems) {
            if (numberOfItems <= 0) {
                onFailure(new IllegalArgumentException("Invalid number of request, must be greater than 0"));
                return;
            }
            if (upstream == Subscriptions.CANCELLED) {
                return;
            }
            Subscriptions.add(pendingRequests, numberOfItems);
            if (pendingRequests.get() == Long.MAX_VALUE) {
                upstream.request(Long.MAX_VALUE);
            } else {
                upstream.request(1L);
            }
        }

        @Override
        public void onItem(IN item) {
            if (upstream == Subscriptions.CANCELLED) {
                return;
            }
            accumulated = accumulator.apply(accumulated, item);
            if (accumulated == null) {
                onFailure(new NullPointerException("The accumulator returned a null value"));
                return;
            }
            if (shouldEmit.test(accumulated)) {
                OUT result = outputTransformer.apply(accumulated, false);
                if (result == null) {
                    onFailure(new NullPointerException("The output transformer returned a null value"));
                    return;
                }
                accumulated = splitter.apply(accumulated);
                if (accumulated == null) {
                    onFailure(new NullPointerException("The splitter returned a null value"));
                    return;
                }
                downstream.onItem(result);
            }
            if (pendingRequests.get() != Long.MAX_VALUE) {
                long remaining = pendingRequests.decrementAndGet();
                if (remaining > 0) {
                    upstream.request(1L);
                }
            }
        }

        @Override
        public void onCompletion() {
            Flow.Subscription subscription = getAndSetUpstreamSubscription(CANCELLED);
            if (subscription != CANCELLED) {
                OUT result = outputTransformer.apply(accumulated, true);
                if (result != null) {
                    downstream.onItem(result);
                }
                downstream.onCompletion();
            }
        }
    }
}
