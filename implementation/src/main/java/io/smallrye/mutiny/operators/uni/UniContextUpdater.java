package io.smallrye.mutiny.operators.uni;

import java.util.function.BiConsumer;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.AbstractUni;
import io.smallrye.mutiny.operators.UniOperator;
import io.smallrye.mutiny.subscription.UniSubscriber;

// TODO
public class UniContextUpdater<I> extends UniOperator<I, I> {

    private final BiConsumer<Context.Updater, ? super I> updater;

    public UniContextUpdater(Uni<? extends I> upstream, BiConsumer<Context.Updater, ? super I> updater) {
        super(upstream);
        this.updater = updater;
    }

    @Override
    public void subscribe(UniSubscriber<? super I> subscriber) {
        AbstractUni.subscribe(upstream(), new UniContextUpdaterProcessor(subscriber));
    }

    private class UniContextUpdaterProcessor extends UniOperatorProcessor<I, I> {

        private final Context context;

        public UniContextUpdaterProcessor(UniSubscriber<? super I> downstream) {
            super(downstream);
            context = downstream.context();
        }

        @Override
        public Context context() {
            return context;
        }

        @Override
        public void onItem(I item) {
            try {
                updater.accept((Context.Updater) context, item);
                super.onItem(item);
            } catch (Throwable err) {
                super.onFailure(err);
            }
        }
    }
}
