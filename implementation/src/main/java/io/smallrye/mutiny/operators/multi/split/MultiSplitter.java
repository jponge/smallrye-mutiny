package io.smallrye.mutiny.operators.multi.split;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public class MultiSplitter<T, K extends Enum<K>> {

    private final Multi<? extends T> upstream;
    private final Function<T, K> splitter;
    private final ConcurrentHashMap<K, SplitMulti.Split> splits;
    private final int requiredNumberOfSubscribers;

    public MultiSplitter(Multi<? extends T> upstream, Class<K> keyType, Function<T, K> splitter) {
        this.upstream = nonNull(upstream, "upstream");
        if (!nonNull(keyType, "keyType").isEnum()) {
            // Note: the Java compiler enforces a type check on keyType being some enum, so this branch is only here for added peace of mind
            throw new IllegalArgumentException("The key type must be that of an enumeration");
        }
        this.splitter = nonNull(splitter, "splitter");
        this.splits = new ConcurrentHashMap<>();
        this.requiredNumberOfSubscribers = keyType.getEnumConstants().length;
    }

    public Multi<T> get(K key) {
        return Infrastructure.onMultiCreation(new SplitMulti(key));
    }

    private enum State {
        INIT,
        AWAITING_SUBSCRIPTION,
        SUBSCRIBED,
        COMPLETED,
        FAILED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);

    private volatile Throwable terminalFailure;

    private Flow.Subscription upstreamSubscription;

    private void onSplitRequest() {
        if (state.get() != State.SUBSCRIBED || splits.size() < requiredNumberOfSubscribers) {
            return;
        }
        for (SplitMulti.Split split : splits.values()) {
            if (split.demand.get() == 0L) {
                return;
            }
        }
        upstreamSubscription.request(1L);
    }

    private void onUpstreamFailure() {
        for (SplitMulti.Split split : splits.values()) {
            split.downstream.onFailure(terminalFailure);
        }
        splits.clear();
    }

    private void onUpstreamCompletion() {
        for (SplitMulti.Split split : splits.values()) {
            split.downstream.onCompletion();
        }
        splits.clear();
    }

    private void onUpstreamItem(T item) {
        try {
            K key = splitter.apply(item);
            if (key == null) {
                throw new NullPointerException("The splitter function returned null");
            }
            SplitMulti.Split target = splits.get(key);
            if (target != null) {
                target.downstream.onItem(item);
                if (splits.size() == requiredNumberOfSubscribers
                        && (target.demand.get() == Long.MAX_VALUE || target.demand.decrementAndGet() > 0L)) {
                    upstreamSubscription.request(1L);
                }
            }
        } catch (Throwable err) {
            terminalFailure = err;
            state.set(State.FAILED);
            onUpstreamFailure();
        }
    }

    // Note: we need a subscriber class because another onCompletion definition exists in Multi
    private class Forwarder implements MultiSubscriber<T> {

        @Override
        public void onItem(T item) {
            if (state.get() != State.SUBSCRIBED) {
                return;
            }
            onUpstreamItem(item);
        }

        @Override
        public void onFailure(Throwable failure) {
            if (state.compareAndSet(State.SUBSCRIBED, State.FAILED)) {
                terminalFailure = failure;
                onUpstreamFailure();
            }
        }

        @Override
        public void onCompletion() {
            if (state.compareAndSet(State.SUBSCRIBED, State.COMPLETED)) {
                onUpstreamCompletion();
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (state.get() != State.AWAITING_SUBSCRIPTION) {
                subscription.cancel();
            } else {
                upstreamSubscription = subscription;
                state.set(State.SUBSCRIBED);
                // In case all splits would be subscribed...
                onSplitRequest();
            }
        }
    }

    private class SplitMulti extends AbstractMulti<T> {

        private final K key;

        SplitMulti(K key) {
            this.key = key;
        }

        @Override
        public void subscribe(MultiSubscriber<? super T> subscriber) {
            nonNull(subscriber, "subscriber");

            // First subscription triggers upstream subscription
            if (state.compareAndSet(State.INIT, State.AWAITING_SUBSCRIPTION)) {
                // TODO handle context passing (first, combined, or empty?)
                upstream.subscribe().withSubscriber(new Forwarder());
            }

            // Early exits
            State stateWhenSubscribing = state.get();
            if (stateWhenSubscribing == State.FAILED) {
                subscriber.onSubscribe(Subscriptions.CANCELLED);
                subscriber.onFailure(terminalFailure);
                return;
            }
            if (stateWhenSubscribing == State.COMPLETED) {
                subscriber.onSubscribe(Subscriptions.CANCELLED);
                subscriber.onCompletion();
                return;
            }

            // Regular subscription path
            Split split = new Split(subscriber);
            Split previous = splits.putIfAbsent(key, split);
            if (previous == null) {
                subscriber.onSubscribe(split);
            } else {
                subscriber.onSubscribe(Subscriptions.CANCELLED);
                subscriber.onError(new IllegalStateException("There is already a subscriber for key " + key));
            }
        }

        private class Split implements Flow.Subscription, ContextSupport {

            MultiSubscriber<? super T> downstream;
            AtomicLong demand = new AtomicLong();

            private Split(MultiSubscriber<? super T> subscriber) {
                this.downstream = subscriber;
            }

            @Override
            public void request(long n) {
                if (n <= 0) {
                    cancel();
                    downstream.onError(Subscriptions.getInvalidRequestException());
                    return;
                }
                Subscriptions.add(demand, n);
                onSplitRequest();
            }

            @Override
            public void cancel() {
                splits.remove(key);
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
