package io.smallrye.mutiny.subscription;

import java.time.Duration;

public class FixedSubscriptionPacer implements SubscriptionPacer {

    private final Request request;

    public FixedSubscriptionPacer(long demand, Duration delay) {
        request = new Request(demand, delay);
    }

    @Override
    public Request apply(Request previousRequest, long observedItemsCount) {
        return request;
    }
}
