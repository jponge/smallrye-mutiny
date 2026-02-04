package io.smallrye.mutiny.helpers.spies;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class UniOnItemOrFailureSpy<T> extends UniSpyBase<T> {

    @Nullable
    private volatile T lastItem;
    @Nullable
    private volatile Throwable lastFailure;

    UniOnItemOrFailureSpy(Uni<T> upstream) {
        super(upstream);
    }

    public boolean hasFailed() {
        return lastFailure != null;
    }

    public T lastItem() {
        return lastItem;
    }

    @Override
    public void reset() {
        super.reset();
        lastItem = null;
        lastFailure = null;
    }

    public Throwable lastFailure() {
        return lastFailure;
    }

    @Override
    public void subscribe(@NotNull UniSubscriber<? super T> downstream) {
        upstream()
                .onItemOrFailure().invoke((item, failure) -> {
                    synchronized (UniOnItemOrFailureSpy.this) {
                        lastItem = item;
                        lastFailure = failure;
                    }
                    incrementInvocationCount();
                })
                .subscribe().withSubscriber(downstream);
    }

    @NotNull
    @Override
    public String toString() {
        return "UniOnItemOrFailureSpy{" +
                "lastItem=" + lastItem +
                ", lastFailure=" + lastFailure +
                "} " + super.toString();
    }
}
