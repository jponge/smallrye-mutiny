package io.smallrye.mutiny.subscription;

import static io.smallrye.mutiny.helpers.ParameterValidation.positive;
import static io.smallrye.mutiny.helpers.ParameterValidation.validate;

import java.time.Duration;

public interface DemandPacer {

    class Request {

        private final long demand;
        private final Duration delay;

        public Request(long demand, Duration delay) {
            this.demand = positive(demand, "demand");
            this.delay = validate(delay, "delay");
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

    Request initial();

    Request apply(Request previousRequest, long observedItemsCount);
}
