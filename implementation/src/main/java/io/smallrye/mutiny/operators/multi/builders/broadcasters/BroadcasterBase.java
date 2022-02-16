package io.smallrye.mutiny.operators.multi.builders.broadcasters;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.groups.UnthrottledBroadcasterConf;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;

abstract class BroadcasterBase<T> extends AbstractMulti<T> {

    protected final Multi<T> multi;
    protected final CopyOnWriteArrayList<BroadcasterSubscription<T>> subscriptions = new CopyOnWriteArrayList<>();
    protected final UnthrottledBroadcasterConf configuration;

    volatile Subscription upstreamSubscription;
    volatile Throwable failure;
    volatile boolean completed;
    AtomicBoolean init = new AtomicBoolean();

    public BroadcasterBase(Multi<T> multi, UnthrottledBroadcasterConf configuration) {
        this.multi = multi;
        this.configuration = configuration;
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> subscriber) {
        if (upstreamSubscription == Subscriptions.CANCELLED) {
            assert (completed) || (failure != null);
            if (completed) {
                Subscriptions.complete(subscriber);
            } else {
                Subscriptions.fail(subscriber, failure);
            }
            return;
        }

        BroadcasterSubscription<T> subscription = newSubscriptionFor(subscriber);
        subscriptions.add(subscription);
        subscriber.onSubscribe(subscription);

        if (init.compareAndSet(false, true)) {
            multi.subscribe(newUpstreamSubscriber(subscriber));
        }
    }

    protected abstract UnthrottledUpstreamSubscriber<T> newUpstreamSubscriber(MultiSubscriber<? super T> subscriber);

    protected abstract BroadcasterSubscription<T> newSubscriptionFor(MultiSubscriber<? super T> subscriber);

    void subscriberHasCancelled(BroadcasterSubscription<T> subscription) {
        subscriptions.remove(subscription);
    }

    void upstreamSubscribedWith(Subscription s) {
        upstreamSubscription = s;
    }

    void dispatchItem(T item) {
        subscriptions.forEach(subscriber -> subscriber.offerItem(item));
    }

    void dispatchFailure(Throwable failure) {
        upstreamSubscription = Subscriptions.CANCELLED;
        this.failure = failure;
        subscriptions.forEach(BroadcasterSubscription::drain);
    }

    void dispatchCompletion() {
        upstreamSubscription = Subscriptions.CANCELLED;
        completed = true;
        subscriptions.forEach(BroadcasterSubscription::drain);
    }
}
