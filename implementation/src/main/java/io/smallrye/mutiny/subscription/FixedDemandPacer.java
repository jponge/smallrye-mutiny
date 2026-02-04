package io.smallrye.mutiny.subscription;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;

/**
 * A demand pacer with a fixed delay / fixed demand.
 */
public class FixedDemandPacer implements DemandPacer {

    @NotNull
    private final Request request;

    public FixedDemandPacer(long demand, @NotNull Duration delay) {
        request = new Request(demand, delay);
    }

    @NotNull
    @Override
    public Request initial() {
        return request;
    }

    @NotNull
    @Override
    public Request apply(Request previousRequest, long observedItemsCount) {
        return request;
    }
}
