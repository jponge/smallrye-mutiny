package io.smallrye.mutiny.operators.multi.builders.broadcasters;

import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

class UnthrottledUpstreamSubscriber<T> implements MultiSubscriber<T>, ContextSupport {

    private final UnthrottledBroadcaster<T> broadcaster;
    private final MultiSubscriber<? super T> triggeringSubscriber;

    UnthrottledUpstreamSubscriber(UnthrottledBroadcaster<T> broadcaster, MultiSubscriber<? super T> triggeringSubscriber) {
        this.triggeringSubscriber = triggeringSubscriber;
        this.broadcaster = broadcaster;
    }

    @Override
    public void onItem(T item) {
        broadcaster.dispatchItem(item);
    }

    @Override
    public void onFailure(Throwable failure) {
        broadcaster.dispatchFailure(failure);
    }

    @Override
    public void onCompletion() {
        broadcaster.dispatchCompletion();
    }

    @Override
    public void onSubscribe(Subscription s) {
        broadcaster.upstreamSubscribedWith(s);
        s.request(Long.MAX_VALUE);
    }

    @Override
    public Context context() {
        if (triggeringSubscriber instanceof ContextSupport) {
            return ((ContextSupport) triggeringSubscriber).context();
        }
        return Context.empty();
    }
}
