package io.smallrye.mutiny.operators.multi.builders.broadcasters;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;

abstract class BroadcasterBase<T> extends AbstractMulti<T> {

    protected final Multi<T> multi;
    private final boolean cancelAfterLastSubscriber;
    private final Duration cancelAfterLastSubscriberDelay;
    protected final CopyOnWriteArrayList<BroadcasterSubscription<T>> subscriptions = new CopyOnWriteArrayList<>();

    volatile Subscription upstreamSubscription;
    volatile Throwable failure;
    volatile boolean completed;
    AtomicBoolean init = new AtomicBoolean();

    public BroadcasterBase(Multi<T> multi, boolean cancelAfterLastSubscriber, Duration cancelAfterLastSubscriberDelay) {
        this.multi = multi;
        this.cancelAfterLastSubscriber = cancelAfterLastSubscriber;
        this.cancelAfterLastSubscriberDelay = cancelAfterLastSubscriberDelay;
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
        if (cancelAfterLastSubscriber && subscriptions.isEmpty()) {
            if (cancelAfterLastSubscriberDelay != null) {
                Infrastructure.getDefaultWorkerPool().schedule(this::checkAndTerminate,
                        cancelAfterLastSubscriberDelay.toNanos(), TimeUnit.NANOSECONDS);
            } else {
                terminate();
            }
        }
    }

    private void checkAndTerminate() {
        if (subscriptions.isEmpty()) {
            terminate();
        }
    }

    private void terminate() {
        upstreamSubscription.cancel();
        upstreamSubscription = Subscriptions.CANCELLED;
        completed = true;
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
