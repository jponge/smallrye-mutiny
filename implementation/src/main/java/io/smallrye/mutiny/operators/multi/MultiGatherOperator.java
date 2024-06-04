package io.smallrye.mutiny.operators.multi;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public class MultiGatherOperator<I, S, O> extends AbstractMultiOperator<I, O> {

    private final Supplier<S> stateSupplier;
    private final BiConsumer<S, I> appender;
    private final Function<S, Multi<O>> extractor;

    public MultiGatherOperator(Multi<I> upstream, Supplier<S> stateSupplier, BiConsumer<S, I> appender,
            Function<S, Multi<O>> extractor) {
        super(upstream);
        this.stateSupplier = nonNull(stateSupplier, "stateSupplier");
        this.appender = nonNull(appender, "appender");
        this.extractor = nonNull(extractor, "extractor");
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> downstream) {
        S state = stateSupplier.get();
        if (state == null) {
            throw new NullPointerException("Supplied state is null");
        }
        Multi<O> multi = upstream.onItem().transformToMultiAndConcatenate(item -> {
            appender.accept(state, item);
            return extractor.apply(state);
        });
        multi.subscribe().withSubscriber(downstream);
    }
}
