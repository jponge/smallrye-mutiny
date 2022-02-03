package io.smallrye.mutiny.operators.multi.builders;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.groups.MultiBroadcast;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MultiReplayPublisher<T> extends AbstractMulti<T> {

    private final int count;
    private final Multi<T> upstreamMulti;
    private final Iterable<T> initialValues;
    private final boolean cancelAfterLastSubscriberLeaves;
    private final Duration cancellationDelay;

    private final AtomicBoolean started = new AtomicBoolean();
    //private final BroadcastProcessor<T> broadcaster = BroadcastProcessor.create();
    private final AtomicLong activeSubscribersCount = new AtomicLong();
    private final Queue<T> itemsToReplay;

    private volatile MultiEmitter<? super T> downstreamEmitter;
    private volatile Subscription upstreamSubscription;
    private volatile Multi<T> broadcastingMulti;

    public MultiReplayPublisher(int count, Multi<T> multi, Iterable<T> initialValues, boolean cancelAfterLastSubscriberLeaves, Duration cancellationDelay) {
        this.count = count;
        this.upstreamMulti = multi;
        this.initialValues = initialValues;
        this.cancelAfterLastSubscriberLeaves = cancelAfterLastSubscriberLeaves;
        this.cancellationDelay = cancellationDelay;
        itemsToReplay = new ConcurrentLinkedDeque<>();
        initialValues.forEach(this::pushToReplayQueue);
    }

    private void pushToReplayQueue(T item) {
        itemsToReplay.add(item);
        if (itemsToReplay.size() > count) {
            itemsToReplay.remove();
        }
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> subscriber) {
        Multi<T> init = Multi.createFrom().iterable(new ArrayList<T>(itemsToReplay));

        synchronized (this) {
            if (broadcastingMulti == null) {
                MultiBroadcast<T> broadcast = upstreamMulti.broadcast();
                if (cancellationDelay != null) {
                    broadcastingMulti = broadcast.withCancellationAfterLastSubscriberDeparture(cancellationDelay).toAtLeast(1);
                } else if (cancelAfterLastSubscriberLeaves) {
                    broadcastingMulti = broadcast.withCancellationAfterLastSubscriberDeparture().toAtLeast(1);
                } else {
                    broadcastingMulti = broadcast.toAtLeast(1);
                }
                broadcastingMulti = broadcastingMulti.onItem().invoke(this::pushToReplayQueue);
            }
        }

        Multi.createBy()
                .concatenating().streams(init, broadcastingMulti)
                .subscribe().withSubscriber(subscriber);

//        activeSubscribersCount.incrementAndGet();
//        Multi<T> init = Multi.createFrom().iterable(new ArrayList<T>(itemsToReplay));
//        Multi<T> rest = broadcaster
////                .onSubscription().invoke(() -> System.out.println("Broadcaster subscribed"))
//                .onCancellation().invoke(() -> {
//                    if (activeSubscribersCount.decrementAndGet() == 0) {
//                        upstreamSubscription.cancel();
//                    }
//                });
//        init.onCompletion().switchTo(() -> {
//            if (started.compareAndSet(false, true)) {
//                upstreamMulti
//                        .onItem().invoke(this::pushToReplayQueue)
//                        .subscribe().withSubscriber(new UpstreamToBroadcaster());
//            }
//            return rest;
//        }).subscribe(subscriber);
//        init
//                .log("A")
//                .onCompletion().invoke(() -> {
//                    if (started.compareAndSet(false, true)) {
//                        upstreamMulti
//                                .onItem().invoke(this::pushToReplayQueue)
//                                .subscribe().withSubscriber(new UpstreamToBroadcaster());
//                    }
//                })
//                .log("B")
//                .onCompletion().switchTo(rest)
//                .log("C")
//                .subscribe(subscriber);
    }

//    private class UpstreamToBroadcaster implements Subscriber<T> {
//
//        @Override
//        public void onSubscribe(Subscription upstreamSubscription) {
//            System.out.println("Sub");
//            MultiReplayPublisher.this.upstreamSubscription = upstreamSubscription;
//            upstreamSubscription.request(Long.MAX_VALUE);
//        }
//
//        @Override
//        public void onNext(T item) {
//            System.out.println("Next");
//            broadcaster.onNext(item);
//        }
//
//        @Override
//        public void onError(Throwable failure) {
//            System.out.println("Err");
//            broadcaster.onError(failure);
//        }
//
//        @Override
//        public void onComplete() {
//            System.out.println("Complete");
//            broadcaster.onComplete();
//        }
//    }
}
