package io.smallrye.mutiny.operators.multi.builders;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class UnthrottledBroadcaster<T> extends AbstractMulti<T> {

    private final Multi<T> multi;
    private final CopyOnWriteArrayList<UnthrottledBroadcasterSubscription> subscribers = new CopyOnWriteArrayList<>();

    private volatile Subscription upstreamSubscription;
    private AtomicBoolean init = new AtomicBoolean();

    private volatile Throwable failure;
    private volatile boolean completed;

    public UnthrottledBroadcaster(Multi<T> multi) {
        this.multi = multi;
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

        UnthrottledBroadcasterSubscription subscription = new UnthrottledBroadcasterSubscription(subscriber);
        subscribers.add(subscription);
        subscriber.onSubscribe(subscription);

        if (init.compareAndSet(false, true)) {
            multi.subscribe(new UpstreamSubscriber(subscriber));
        }
    }

    private class UpstreamSubscriber implements MultiSubscriber<T>, ContextSupport {

        private final MultiSubscriber<? super T> triggeringSubscriber;

        private UpstreamSubscriber(MultiSubscriber<? super T> triggeringSubscriber) {
            this.triggeringSubscriber = triggeringSubscriber;
        }

        @Override
        public void onItem(T item) {
            subscribers.forEach(subscriber -> {
                subscriber.itemsQueue.add(item);
                subscriber.drain();
            });
        }

        @Override
        public void onFailure(Throwable failure) {
            upstreamSubscription = Subscriptions.CANCELLED;
            UnthrottledBroadcaster.this.failure = failure;
            subscribers.forEach(UnthrottledBroadcasterSubscription::drain);
        }

        @Override
        public void onCompletion() {
            upstreamSubscription = Subscriptions.CANCELLED;
            UnthrottledBroadcaster.this.completed = true;
            subscribers.forEach(UnthrottledBroadcasterSubscription::drain);
        }

        @Override
        public void onSubscribe(Subscription s) {
            upstreamSubscription = s;
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

    private class UnthrottledBroadcasterSubscription implements Subscription {

        private final MultiSubscriber<? super T> subscriber;

        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicLong demand = new AtomicLong();

        private final ConcurrentLinkedDeque<T> itemsQueue = new ConcurrentLinkedDeque<>();

        UnthrottledBroadcasterSubscription(MultiSubscriber<? super T> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (cancelled.get()) {
                return;
            }
            if (n <= 0) {
                throw Subscriptions.getInvalidRequestException();
            }
            Subscriptions.add(demand, n);
            drain();
        }

        private final AtomicInteger wip = new AtomicInteger();

        private void drain() {
            if (wip.getAndIncrement() > 0) {
                // Another thread is working
                return;
            }
            while (true) {
                long max = demand.get();
                boolean unboubded = (max == Long.MAX_VALUE);
                long emitted = 0L;

                for (long i = 0L; i < max; i++) {
                    // Cancellation
                    if (cancelled.get()) {
                        return;
                    }

                    // Dispatch an item or a terminal event
                    if (itemsQueue.isEmpty()) {
                        if (completed) {
                            cancel();
                            subscriber.onComplete();
                            return;
                        } else if (failure != null) {
                            cancel();
                            subscriber.onFailure(failure);
                            return;
                        }
                    } else {
                        subscriber.onItem(itemsQueue.pop());
                        emitted++;
                    }
                }

                // Decrease remaining demand and exit conditions
                boolean emptyDemand = !unboubded && (demand.addAndGet(-emitted) == 0L);
                if (wip.decrementAndGet() == 0 || emptyDemand) {
                    return;
                }
            }
        }

        @Override
        public void cancel() {
            if (cancelled.compareAndSet(false, true)) {
                subscribers.remove(this);
            }
        }
    }
}
