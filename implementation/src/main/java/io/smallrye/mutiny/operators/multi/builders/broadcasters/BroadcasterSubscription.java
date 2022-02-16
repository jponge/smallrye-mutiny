package io.smallrye.mutiny.operators.multi.builders.broadcasters;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscription;

import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.subscription.MultiSubscriber;

class BroadcasterSubscription<T> implements Subscription {

    private final MultiSubscriber<? super T> subscriber;
    private final UnthrottledBroadcaster<T> broadcaster;

    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicLong demand = new AtomicLong();

    private final Queue<T> itemsQueue;

    BroadcasterSubscription(UnthrottledBroadcaster<T> broadcaster, MultiSubscriber<? super T> subscriber,
            int subscriberInitialQueueSize) {
        this.broadcaster = broadcaster;
        this.subscriber = subscriber;
        this.itemsQueue = Queues.<T> get(subscriberInitialQueueSize).get();
    }

    public void offerItem(T item) {
        if (!cancelled.get()) {
            itemsQueue.offer(item);
            drain();
        }
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

    final AtomicInteger wip = new AtomicInteger();

    void drain() {
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
                    if (broadcaster.completed) {
                        cancel();
                        subscriber.onComplete();
                        return;
                    } else if (broadcaster.failure != null) {
                        cancel();
                        subscriber.onFailure(broadcaster.failure);
                        return;
                    }
                } else {
                    subscriber.onItem(itemsQueue.poll());
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
            broadcaster.subscriberHasCancelled(this);
        }
    }
}
