package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class MultiSplitter<T, K extends Enum<K>> {

    private final Multi<? extends T> upstream;
    private final Function<T, K> splitter;
    private final Map<K, SplitMulti.Split> splits;

    public MultiSplitter(Multi<? extends T> upstream, Class<K> keyType, Function<T, K> splitter) {
        this.upstream = upstream;
        this.splitter = splitter;
        this.splits = Collections.synchronizedMap(new EnumMap<>(keyType));
    }

    public Multi<T> get(K key) {
        return new SplitMulti(key);
    }

    private class SplitMulti extends AbstractMulti<T> {

        final K key;

        SplitMulti(K key) {
            this.key = key;
        }

        @Override
        public void subscribe(MultiSubscriber<? super T> subscriber) {
            Split split = null;
            synchronized (splits) {
                if (!splits.containsKey(key)) {
                    split = new Split();
                    splits.put(key, split);
                }
            }
            if (split != null) {
                subscriber.onSubscribe(split);
            } else {
                subscriber.onSubscribe(Subscriptions.CANCELLED);
                subscriber.onError(new IllegalStateException("There is already a subscriber for key " + key));
            }
        }

        private class Split implements Flow.Subscription, ContextSupport {
            MultiSubscriber<? super T> downstream;
            AtomicLong demand = new AtomicLong();

            @Override
            public void request(long n) {
                if (n <= 0) {
                    cancel();
                    downstream.onError(Subscriptions.getInvalidRequestException());
                    return;
                }
                Subscriptions.add(demand, n);
                // TODO notify coordinator
            }

            @Override
            public void cancel() {
                splits.remove(key);
                // TODO notify coordinator
            }

            @Override
            public Context context() {
                if (downstream instanceof ContextSupport) {
                    return ((ContextSupport) downstream).context();
                } else {
                    return Context.empty();
                }
            }
        }
    }
}
