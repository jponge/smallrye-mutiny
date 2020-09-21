package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.HashSet;
import java.util.function.BooleanSupplier;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

public class UniCache<T> extends UniOperator<T, T> implements UniSubscriber<T> {

    private enum State {
        INIT,
        SUBSCRIBING,
        SUBSCRIBED,
        RESOLVED
    }

    private final HashSet<UniSubscriber<? super T>> subscribers = new HashSet<>();

    private volatile State state = State.INIT;
    private volatile UniSubscription upstreamSubscription;
    private volatile T item;
    private volatile Throwable failure;

    private final BooleanSupplier invalidationGuard;

    public UniCache(Uni<? extends T> upstream, BooleanSupplier invalidationGuard) {
        super(nonNull(upstream, "upstream"));
        this.invalidationGuard = nonNull(invalidationGuard, "invalidationGuard");
    }

    public UniCache(Uni<? extends T> upstream) {
        this(upstream, () -> true);
    }

    @Override
    protected void subscribing(UniSerializedSubscriber<? super T> subscriber) {
        synchronized (this) {
            subscribers.add(subscriber);
            switch (state) {
                case RESOLVED:
                    if (invalidationGuard.getAsBoolean()) {
                        passSubscriptionAndForward(subscriber);
                        break;
                    } else {
                        state = State.INIT;
                    }
                case INIT:
                    state = State.SUBSCRIBING;
                    AbstractUni.subscribe(upstream(), this);
                    break;
                case SUBSCRIBING:
                    break;
                case SUBSCRIBED:
                    passSubscription(subscriber);
                default:
                    throw new IllegalStateException("We are in state " + state);
            }
        }
    }

    @Override
    public void onSubscribe(UniSubscription subscription) {
        synchronized (this) {
            if (upstreamSubscription != null) {
                throw new IllegalStateException("Invalid state " + state + " - received a second subscription from source");
            }
            if (state != State.SUBSCRIBING) {
                throw new IllegalStateException("Invalid state " + state + " - should be SUBSCRIBING");
            }
            state = State.SUBSCRIBED;
            this.upstreamSubscription = subscription;
            dispatchSubscriptions();
        }
    }

    @Override
    public void onItem(T item) {
        synchronized (this) {
            if (state != State.SUBSCRIBED) {
                throw new IllegalStateException(
                        "Invalid state " + state + " - received item while we where not in the SUBSCRIBED state");
            }
            state = State.RESOLVED;
            this.item = item;
            forwardResultToSubscribers();
        }
    }

    @Override
    public void onFailure(Throwable failure) {
        synchronized (this) {
            if (state != State.SUBSCRIBED) {
                throw new IllegalStateException(
                        "Invalid state " + state + " - received failure while we where not in the SUBSCRIBED state");
            }
            state = State.RESOLVED;
            this.failure = failure;
            forwardResultToSubscribers();
        }
    }

    private void forwardResultToSubscribers() {
        for (UniSubscriber<? super T> subscriber : subscribers) {
            forwardResult(subscriber);
        }
        subscribers.clear();
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
        passSubscription(subscriber);
        forwardResult(subscriber);
        subscribers.remove(subscriber);
    }

    private void dispatchSubscriptions() {
        for (UniSubscriber<? super T> subscriber : subscribers) {
            passSubscription(subscriber);
        }
        this.upstreamSubscription = null;
    }

    private void passSubscription(UniSubscriber<? super T> subscriber) {
        subscriber.onSubscribe(() -> {
            synchronized (UniCache.this) {
                subscribers.remove(subscriber);
            }
        });
    }
}
