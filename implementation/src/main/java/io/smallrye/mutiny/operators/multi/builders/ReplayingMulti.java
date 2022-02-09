package io.smallrye.mutiny.operators.multi.builders;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.subscription.SwitchableSubscriptionSubscriber;
import org.reactivestreams.Subscriber;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ReplayingMulti<T> extends AbstractMulti<T> {

    private final Multi<T> broadcastingMulti;
    private final int replayCount = 5;

    private final AtomicBoolean initialSubscriptionPerformed = new AtomicBoolean();
    private final ReplayQueue replayQueue;

    public ReplayingMulti(Multi<T> multi) {
        this.broadcastingMulti = multi
                .onItem().invoke(this::recordItemToReplay)
                .broadcast().toAllSubscribers();
        this.replayQueue = new ReplayQueue(replayCount);
    }

    private void recordItemToReplay(T item) {
        replayQueue.push(item);
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> downstream) {
        if (initialSubscriptionPerformed.compareAndSet(false, true)) {
            broadcastingMulti.subscribe(downstream);
        } else {
            downstream.onSubscribe(new ReplaySubscription(downstream));
        }
    }

    private enum ReplayState {
        INIT,
        REPLAYED
    }

    private class ReplaySubscription extends SwitchableSubscriptionSubscriber<T> {

        private final AtomicReference<ReplayState> state = new AtomicReference<>(ReplayState.INIT);

        ReplaySubscription(MultiSubscriber<? super T> downstream) {
            super(downstream);
        }

        @Override
        public void onItem(T item) {
            if (!isCancelled()) {
                downstream.onItem(item);
            }
        }

        @Override
        public void onError(Throwable t) {
            // TODO
            super.onError(t);
        }

        @Override
        public void onComplete() {
            // TODO
            super.onComplete();
        }

        @Override
        public void request(long n) {
            super.request(n);
            if (!isCancelled() && state.get() == ReplayState.INIT && replayDemandIsMet() && state.compareAndSet(ReplayState.INIT, ReplayState.REPLAYED)) {
                replay();
                switchToBroadcaster();
            }
        }

        private void replay() {
            System.out.println(">> r e p l a y <<");
            emitted(replayQueue.replay(downstream));
        }

        private boolean replayDemandIsMet() {
            return requested() >= replayCount;
        }

        private void switchToBroadcaster() {
            broadcastingMulti.subscribe(this);
        }
    }

    static class ReplayQueue {

        int head;
        int tail;
        Object[] ringBuffer;

        ReplayQueue(int size) {
            this.ringBuffer = new Object[size];
        }

        void push(Object item) {
            synchronized (this) {
                head = advance(head);
                ringBuffer[head] = item;
                if (head == tail) {
                    tail = advance(tail);
                }
            }
        }

        int replay(Subscriber subscriber) {
            synchronized (this) {
                int index = tail;
                int count = 0;
                System.out.println("replay = " + Arrays.toString(ringBuffer));
                System.out.println("         @" + tail + " -- " + head);
                while (index != head) {
                    subscriber.onNext(ringBuffer[index]);
                    index = advance(index);
                    count = count + 1;
                }
                return count;
            }
        }

        private int advance(int index) {
            return (index + 1) % ringBuffer.length;
        }
    }
}
