package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

// TODO context support
public class MultiGatherToState<T, S, O> extends AbstractMultiOperator<T, O> {

    private final Supplier<S> stateSupplier;
    private final BiFunction<S, T, S> appender;
    private final Function<S, Multi<O>> extractor;

    public MultiGatherToState(Multi<? extends T> upstream, Supplier<S> stateSupplier, BiFunction<S, T, S> appender, Function<S, Multi<O>> extractor) {
        super(upstream);
        this.stateSupplier = stateSupplier;
        this.appender = appender;
        this.extractor = extractor;
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> downstream) {
        S state = stateSupplier.get();
        if (state == null) {
            throw new NullPointerException("Supplied state is null");
        }
        upstream.onItem().transformToMultiAndConcatenate(new Function<T, Multi<O>>() {
            S innerState = state;

            @Override
            public Multi<O> apply(T item) {
                innerState = appender.apply(innerState, item);
                return extractor.apply(innerState);
            }
        }).subscribe().withSubscriber(downstream);
    }
}
