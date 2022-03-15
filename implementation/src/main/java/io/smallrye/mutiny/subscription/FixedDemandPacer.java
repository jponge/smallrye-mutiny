package io.smallrye.mutiny.subscription;

import java.time.Duration;

public class FixedDemandPacer implements DemandPacer {

    private final Request request;

    public FixedDemandPacer(long demand, Duration delay) {
        request = new Request(demand, delay);
    }

    @Override
    public Request initial() {
        return request;
    }

    @Override
    public Request apply(Request previousRequest, long observedItemsCount) {
        return request;
    }
}
