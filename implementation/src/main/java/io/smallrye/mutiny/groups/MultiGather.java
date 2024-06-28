package io.smallrye.mutiny.groups;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.MultiGatherToState;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class MultiGather<T> {

    private final Multi<T> upstream;

    public MultiGather(Multi<T> upstream) {
        this.upstream = upstream;
    }

    @CheckReturnValue
    public <S> StateGatherer<T, S> toState(Supplier<S> supplier) {
        return new StateGatherer<>(upstream, supplier);
    }

    @CheckReturnValue
    public <O> Multi<O> toStream(Supplier<MultiAndAppender<T, O>> supplier) {
        throw new UnsupportedOperationException("To be implemented");
    }

    public static class MultiAndAppender<T, O> {

        private final Multi<O> multi;
        private final Consumer<T> appender;

        public MultiAndAppender(Multi<O> multi, Consumer<T> appender) {
            this.multi = multi;
            this.appender = appender;
        }

        public Multi<O> multi() {
            return multi;
        }

        public Consumer<T> appender() {
            return appender;
        }
    }

    public static class StateGatherer<T, S> {

        private final Multi<T> upstream;
        private final Supplier<S> stateSupplier;

        public StateGatherer(Multi<T> upstream, Supplier<S> stateSupplier) {
            this.upstream = upstream;
            this.stateSupplier = stateSupplier;
        }

        @CheckReturnValue
        public StateGathererAppender<T, S> withStateUpdater(BiFunction<S, T, S> appender) {
            return new StateGathererAppender<>(upstream, stateSupplier, appender);
        }
    }

    public static class StateGathererAppender<T, S> {

        private final Multi<T> upstream;
        private final Supplier<S> stateSupplier;
        private final BiFunction<S, T, S> appender;

        public StateGathererAppender(Multi<T> upstream, Supplier<S> stateSupplier, BiFunction<S, T, S> appender) {
            this.upstream = upstream;
            this.stateSupplier = stateSupplier;
            this.appender = appender;
        }

        @CheckReturnValue
        public <O> Multi<O> withExtractor(Function<S, Multi<O>> extractor) {
            return new MultiGatherToState<>(upstream, stateSupplier, appender, extractor);
        }
    }
}
