package io.smallrye.mutiny.groups;

import java.time.Duration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.operators.multi.builders.broadcasters.UnthrottledBroadcaster;

// TODO
public class MultiBroadcaster {

    public <T> Multi<T> unthrottled(Multi<T> multi) {
        return new UnthrottledBroadcaster<>(multi, UnthrottledConf.newBuilder().build());
    }

    public <T> Multi<T> unthrottled(Multi<T> multi, UnthrottledConf configuration) {
        return new UnthrottledBroadcaster<>(multi, configuration);
    }

    public static class UnthrottledConf {

        private final int subscriberInitialQueueSize;
        private final boolean cancelAfterLastSubscriber;
        private final Duration cancelAfterLastSubscriberDelay;

        private UnthrottledConf(Builder builder) {
            subscriberInitialQueueSize = builder.subscriberInitialQueueSize;
            cancelAfterLastSubscriber = builder.cancelAfterLastSubscriber;
            cancelAfterLastSubscriberDelay = builder.cancelAfterLastSubscriberDelay;
        }

        public int subscriberInitialQueueSize() {
            return subscriberInitialQueueSize;
        }

        public boolean cancelAfterLastSubscriber() {
            return cancelAfterLastSubscriber;
        }

        public Duration cancelAfterLastSubscriberDelay() {
            return cancelAfterLastSubscriberDelay;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static final class Builder {

            private int subscriberInitialQueueSize = Queues.BUFFER_XS;
            private boolean cancelAfterLastSubscriber = false;
            private Duration cancelAfterLastSubscriberDelay = null;

            private Builder() {
            }

            public Builder subscriberInitialQueueSize(int subscriberInitialQueueSize) {
                this.subscriberInitialQueueSize = subscriberInitialQueueSize;
                return this;
            }

            public Builder cancelAfterLastSubscriber(boolean cancelAfterLastSubscriber) {
                this.cancelAfterLastSubscriber = cancelAfterLastSubscriber;
                return this;
            }

            public Builder cancelAfterLastSubscriberDelay(Duration cancelAfterLastSubscriberDelay) {
                this.cancelAfterLastSubscriber = true;
                this.cancelAfterLastSubscriberDelay = cancelAfterLastSubscriberDelay;
                return this;
            }

            public UnthrottledConf build() {
                return new UnthrottledConf(this);
            }
        }
    }

    public static class ThrottledConf {

        //private final int subscriberInitialQueueSize;
        //private final boolean cancelAfterLastSubscriber;
        //private final Duration cancelAfterLastSubscriberDelay;

    }
}
