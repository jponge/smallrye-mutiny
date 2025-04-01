package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.MultiGather;
import io.smallrye.mutiny.tuples.Tuple2;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class MultiOnItemGather<I> {

    private final Multi<I> upstream;

    public MultiOnItemGather(Multi<I> upstream) {
        this.upstream = upstream;
    }

    public <ACC> InitialAccumulatorStep<ACC> withAccumulator(Supplier<ACC> initialAccumulatorSupplier) {
        return new InitialAccumulatorStep<>(initialAccumulatorSupplier);
    }

    public class InitialAccumulatorStep<ACC> {
        private final Supplier<ACC> initialAccumulatorSupplier;

        private InitialAccumulatorStep(Supplier<ACC> initialAccumulatorSupplier) {
            this.initialAccumulatorSupplier = initialAccumulatorSupplier;
        }

        public ExtractStep<ACC> accumulate(BiFunction<ACC, I, ACC> accumulator) {
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

        public <O> FinalizerStep<ACC, O> extract(Function<ACC, Optional<Tuple2<ACC, O>>> extractor) {
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

        public Multi<O> finalize(Function<ACC, Optional<O>> finalizer) {
            return new MultiGather<>(upstream, initialAccumulatorSupplier, accumulator, extractor, finalizer);
        }
    }
}
