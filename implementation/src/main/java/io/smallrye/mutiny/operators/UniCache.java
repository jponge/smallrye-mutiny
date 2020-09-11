package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

public class UniCache<T> extends UniOperator<T, T> implements UniSubscriber<T> {

    private static final int INIT = 0;
    private static final int SUBSCRIBING = 1;
    private static final int SUBSCRIBED = 2;
    private static final int RESOLVED = 3;

    private final AtomicInteger state = new AtomicInteger(INIT);
    private final AtomicReference<UniSubscription> upstreamSubscription = new AtomicReference<>();
    private final HashSet<UniSubscriber<? super T>> subscribers = new HashSet<>();

    private volatile T item;
    private volatile Throwable failure;

    public UniCache(Uni<? extends T> upstream) {
        super(nonNull(upstream, "upstream"));
    }

    @Override
    protected void subscribing(UniSerializedSubscriber<? super T> subscriber) {
        synchronized (state) {
            subscribers.add(subscriber);
        }
        if (state.compareAndSet(INIT, SUBSCRIBING)) {
            AbstractUni.subscribe(upstream(), this);
        } else if (state.get() == SUBSCRIBED) {
            passSubscription(subscriber);
        } else if (state.get() == RESOLVED) {
            passSubscriptionAndForward(subscriber);
        } else {
            throw new IllegalStateException("Unknown state " + state.get());
        }
    }

    private void forwardResultToSubscribers() {
        synchronized (state) {
            for (UniSubscriber<? super T> subscriber : subscribers) {
                forwardResult(subscriber);
            }
            subscribers.clear();
        }
    }

    private void forwardResult(UniSubscriber<? super T> subscriber) {
        if (subscribers.contains(subscriber)) {
            if (failure != null) {
                subscriber.onFailure(failure);
            } else {
                subscriber.onItem(item);
            }
        }
    }

    private void passSubscriptionAndForward(UniSerializedSubscriber<? super T> subscriber) {
        synchronized (state) {
            passSubscription(subscriber);
            forwardResult(subscriber);
            subscribers.remove(subscriber);
        }
    }

    private void dispatchSubscriptions() {
        synchronized (state) {
            for (UniSubscriber<? super T> subscriber : subscribers) {
                passSubscription(subscriber);
            }
        }
    }

    private void passSubscription(UniSubscriber<? super T> subscriber) {
        synchronized (state) {
            subscriber.onSubscribe(() -> {
                synchronized (state) {
                    subscribers.remove(subscriber);
                }
            });
        }
    }

    @Override
    public void onSubscribe(UniSubscription subscription) {
        if (upstreamSubscription.compareAndSet(null, subscription)) {
            if (!state.compareAndSet(SUBSCRIBING, SUBSCRIBED)) {
                throw new IllegalStateException("Invalid state " + state.get() + " - should be SUBSCRIBING");
            }
            dispatchSubscriptions();
        } else {
            throw new IllegalStateException("Invalid state " + state.get() + " - received a second subscription from source");
        }
    }

    @Override
    public void onItem(T item) {
        if (state.get() != SUBSCRIBED) {
            throw new IllegalStateException(
                    "Invalid state " + state.get() + " - received item while we where not in the SUBSCRIBED state");
        }
        state.set(RESOLVED);
        this.item = item;
        forwardResultToSubscribers();
    }

    @Override
    public void onFailure(Throwable failure) {
        if (state.get() != SUBSCRIBED) {
            throw new IllegalStateException(
                    "Invalid state " + state.get() + " - received failure while we where not in the SUBSCRIBED state");
        }
        state.set(RESOLVED);
        this.failure = failure;
        forwardResultToSubscribers();
    }
}
