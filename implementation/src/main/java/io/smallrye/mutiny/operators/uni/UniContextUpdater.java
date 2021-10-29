package io.smallrye.mutiny.operators.uni;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.ContextUpdater;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.context.ContextView;
import io.smallrye.mutiny.context.UpdatableContext;
import io.smallrye.mutiny.operators.AbstractUni;
import io.smallrye.mutiny.operators.UniOperator;
import io.smallrye.mutiny.subscription.UniSubscriber;

import java.util.function.BiConsumer;

// TODO
public class UniContextUpdater<I> extends UniOperator<I, I> {

    private final BiConsumer<ContextUpdater, ? super I> updater;

    public UniContextUpdater(Uni<? extends I> upstream, BiConsumer<ContextUpdater, ? super I> updater) {
        super(upstream);
        this.updater = updater;
    }

    @Override
    public void subscribe(UniSubscriber<? super I> subscriber) {
        AbstractUni.subscribe(upstream(), new UniContextUpdaterProcessor(subscriber));
    }

    private class UniContextUpdaterProcessor extends UniOperatorProcessor<I, I> {

        private final UpdatableContext updatableContext;
        private final ContextView contextView;

        public UniContextUpdaterProcessor(UniSubscriber<? super I> downstream) {
            super(downstream);
            this.contextView = (ContextView) downstream.context();
            this.updatableContext = this.contextView.updatableContext();
        }

        @Override
        public Context context() {
            return contextView;
        }

        @Override
        public void onItem(I item) {
            try {
                updater.accept(updatableContext, item);
                super.onItem(item);
            } catch (Throwable err) {
                super.onFailure(err);
            }
        }
    }
}
