package io.smallrye.mutiny.subscription;

import java.time.Duration;

@FunctionalInterface
public interface SubscriptionPacer {

    class Request {

        private final long demand;
        private final Duration delay;

        public Request(long demand, Duration delay) {
            this.demand = demand;
            this.delay = delay;
        }

        public long demand() {
            return demand;
        }

        public Duration delay() {
            return delay;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "demand=" + demand +
                    ", delay=" + delay +
                    '}';
        }
    }

    Request apply(Request previousRequest, long observedItemsCount);
}
