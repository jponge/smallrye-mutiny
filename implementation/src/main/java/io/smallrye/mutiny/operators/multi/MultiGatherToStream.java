package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.groups.MultiGather;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

import java.util.function.Consumer;
import java.util.function.Supplier;

// TODO context support
public class MultiGatherToStream<T, S, O> extends AbstractMultiOperator<T, O> {

    private final Supplier<MultiGather.MultiAndAppender<T, O>> supplier;

    public MultiGatherToStream(Multi<? extends T> upstream, Supplier<MultiGather.MultiAndAppender<T, O>> supplier) {
        super(upstream);
        this.supplier = supplier;
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> subscriber) {
        MultiGather.MultiAndAppender<T, O> config = supplier.get();
        // TODO fail on null
        Multi<O> multi = config.multi();
        Consumer<T> appender = config.appender();

        Operator operator = new Operator(multi, appender, subscriber);
        upstream.subscribe().withSubscriber(operator);
        multi.onCancellation().invoke(operator::cancel) // TODO check correctness
                .subscribe().withSubscriber(subscriber);
    }

    private class Operator extends MultiOperatorProcessor<T, O> {

        private final Multi<O> multi;
        private final Consumer<T> appender;

        public Operator(Multi<O> multi, Consumer<T> appender, MultiSubscriber<? super O> downstream) {
            super(downstream);
            this.multi = multi;
            this.appender = appender;
        }

        @Override
        public void onItem(T item) {
            appender.accept(item);
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
}
