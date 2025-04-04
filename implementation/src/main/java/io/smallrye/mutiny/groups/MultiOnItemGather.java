package io.smallrye.mutiny.groups;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.MultiGather;
import io.smallrye.mutiny.tuples.Tuple2;

@Experimental("This API is still being designed and may change in the future")
public class MultiOnItemGather<I> {

    private final Multi<I> upstream;

    public MultiOnItemGather(Multi<I> upstream) {
        this.upstream = upstream;
    }

    /**
     * Specifies the initial accumulator supplier.
     * <p>
     * The accumulator is used to accumulate the items emitted by the upstream.
     *
     * @param initialAccumulatorSupplier the initial accumulator supplier, the returned value cannot be {@code null}
     * @param <ACC> the type of the accumulator
     * @return the next step in the builder
     */
    public <ACC> InitialAccumulatorStep<ACC> into(Supplier<ACC> initialAccumulatorSupplier) {
        nonNull(initialAccumulatorSupplier, "initialAccumulatorSupplier");
        return new InitialAccumulatorStep<>(initialAccumulatorSupplier);
    }

    public class InitialAccumulatorStep<ACC> {
        private final Supplier<ACC> initialAccumulatorSupplier;

        private InitialAccumulatorStep(Supplier<ACC> initialAccumulatorSupplier) {
            this.initialAccumulatorSupplier = initialAccumulatorSupplier;
        }

        /**
         * Specifies the accumulator function.
         * <p>
         * The accumulator function is used to accumulate the items emitted by the upstream.
         *
         * @param accumulator the accumulator function, which takes the current accumulator and the item emitted by the
         *        upstream, and returns the new accumulator
         * @return the next step in the builder
         */
        public ExtractStep<ACC> accumulate(BiFunction<ACC, I, ACC> accumulator) {
            nonNull(accumulator, "accumulator");
            return new ExtractStep<>(initialAccumulatorSupplier, accumulator);
        }
    }

    public class ExtractStep<ACC> {
        private final Supplier<ACC> initialAccumulatorSupplier;
        private final BiFunction<ACC, I, ACC> accumulator;

        private ExtractStep(Supplier<ACC> initialAccumulatorSupplier, BiFunction<ACC, I, ACC> accumulator) {
            this.initialAccumulatorSupplier = initialAccumulatorSupplier;
            this.accumulator = accumulator;
        }

        /**
         * Specifies the extractor function.
         * <p>
         * The extractor function is used to extract the items from the accumulator.
         * When the extractor function returns an empty {@link Optional}, no value is emitted.
         * When the extractor function returns a non-empty {@link Optional}, the value is emitted, and the accumulator is
         * updated.
         * This is done by returning a {@link Tuple2} containing the new accumulator and the value to emit.
         *
         * @param extractor the extractor function, which takes the current accumulator and returns an {@link Optional}
         *        containing a {@link Tuple2} with the new accumulator and the value to emit
         * @param <O> the type of the value to emit
         * @return the next step in the builder
         */
        public <O> FinalizerStep<ACC, O> extract(Function<ACC, Optional<Tuple2<ACC, O>>> extractor) {
            nonNull(extractor, "extractor");
            return new FinalizerStep<>(initialAccumulatorSupplier, accumulator, extractor);
        }
    }

    public class FinalizerStep<ACC, O> {
        private final Supplier<ACC> initialAccumulatorSupplier;
        private final BiFunction<ACC, I, ACC> accumulator;
        private final Function<ACC, Optional<Tuple2<ACC, O>>> extractor;

        private FinalizerStep(Supplier<ACC> initialAccumulatorSupplier,
                BiFunction<ACC, I, ACC> accumulator,
                Function<ACC, Optional<Tuple2<ACC, O>>> extractor) {
            this.initialAccumulatorSupplier = initialAccumulatorSupplier;
            this.accumulator = accumulator;
            this.extractor = extractor;
        }

        /**
         * Specifies the finalizer function.
         * <p>
         * The finalizer function is used to emit the final value upon completion of the upstream and when there are no more
         * items that can be extracted from the accumulator.
         * When the finalizer function returns an empty {@link Optional}, no value is emitted before the completion signal.
         * When the finalizer function returns a non-empty {@link Optional}, the value is emitted before the completion signal.
         *
         * @param finalizer the finalizer function, which takes the current accumulator and returns an {@link Optional}
         *        containing the value to emit before the completion signal, if any
         * @return the gathering {@link Multi}
         */
        public Multi<O> finalize(Function<ACC, Optional<O>> finalizer) {
            nonNull(finalizer, "finalizer");
            return new MultiGather<>(upstream, initialAccumulatorSupplier, accumulator, extractor, finalizer);
        }
    }
}
