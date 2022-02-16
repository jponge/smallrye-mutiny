package io.smallrye.mutiny.operators.multi.builders.broadcasters;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.groups.MultiBroadcaster;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public class UnthrottledBroadcaster<T> extends BroadcasterBase<T> {

    private final MultiBroadcaster.UnthrottledConf configuration;

    public UnthrottledBroadcaster(Multi<T> multi, MultiBroadcaster.UnthrottledConf configuration) {
        super(multi, configuration.cancelAfterLastSubscriber(), configuration.cancelAfterLastSubscriberDelay());
        this.configuration = configuration;
    }

    @Override
    protected UnthrottledUpstreamSubscriber<T> newUpstreamSubscriber(MultiSubscriber<? super T> subscriber) {
        return new UnthrottledUpstreamSubscriber<>(this, subscriber);
    }

    @Override
    protected BroadcasterSubscription<T> newSubscriptionFor(MultiSubscriber<? super T> subscriber) {
        return new BroadcasterSubscription<>(this, subscriber, configuration.subscriberInitialQueueSize());
    }

}
