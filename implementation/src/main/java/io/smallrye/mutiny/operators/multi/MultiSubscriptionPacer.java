package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.subscription.SubscriptionPacer;

public class MultiSubscriptionPacer<T> extends AbstractMultiOperator<T, T> {

    private final SubscriptionPacer pacer;

    public MultiSubscriptionPacer(Multi<? extends T> upstream, SubscriptionPacer pacer) {
        super(upstream);
        this.pacer = pacer;
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> subscriber) {
        throw new UnsupportedOperationException("To be implemented");
    }


}
