package io.smallrye.mutiny.helpers.spies;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class UniOnItemSpy<T> extends UniSpyBase<T> {

    @Nullable
    private volatile T lastItem;

    UniOnItemSpy(Uni<T> upstream) {
        super(upstream);
    }

    public T lastItem() {
        return lastItem;
    }

    @Override
    public void reset() {
        super.reset();
        lastItem = null;
    }

    @Override
    public void subscribe(@NotNull UniSubscriber<? super T> downstream) {
        upstream()
                .onItem().invoke(item -> {
                    this.lastItem = item;
                    incrementInvocationCount();
                })
                .subscribe().withSubscriber(downstream);
    }

    @NotNull
    @Override
    public String toString() {
        return "UniOnItemSpy{" +
                "lastItem=" + lastItem +
                "} " + super.toString();
    }
}
