package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.helpers.queues.Queues;

public abstract class BroadcasterConf<T extends BroadcasterConf<T>> {

    public static UnthrottledBroadcasterConf create() {
        return new UnthrottledBroadcasterConf();
    }

    private int subscriberInitialQueueSize = Queues.BUFFER_XS;
    private boolean cancelAfterLastSubscriber = false;

    public T withSubscriberInitialQueueSize(int subscriberInitialQueueSize) {
        this.subscriberInitialQueueSize = subscriberInitialQueueSize;
        return (T) this;
    }

    public T cancelAfterLastSubscriber(boolean value) {
        this.cancelAfterLastSubscriber = value;
        return (T) this;
    }

    public int subscriberInitialQueueSize() {
        return subscriberInitialQueueSize;
    }

    public boolean cancelAfterLastSubscriber() {
        return cancelAfterLastSubscriber;
    }
}
