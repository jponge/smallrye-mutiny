package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.DemandPacer;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class MultiDemandPacingTest {

    @Test
    void smoke_test() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        Multi.createFrom().range(1, 100)
                .paceDemand().on(Infrastructure.getDefaultWorkerPool()).using(new FixedDemandPacer(10, Duration.ofSeconds(1)))
                .subscribe().with(System.out::println, Throwable::printStackTrace, latch::countDown);

        latch.await(1, TimeUnit.MINUTES);
    }

    @Test
    void smoke_test_custom_pacer() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        DemandPacer pacer = new DemandPacer() {
            @Override
            public Request initial() {
                return new Request(1, Duration.ofMillis(500));
            }

            @Override
            public Request apply(Request previousRequest, long observedItemsCount) {
                long newDemand = previousRequest.demand();
                if (observedItemsCount > 10) {
                    newDemand = Math.max(1, observedItemsCount / 2);
                } else {
                    newDemand = previousRequest.demand() * 2;
                }
                return new Request(newDemand, Duration.ofMillis(500));
            }
        };

        Multi.createFrom().range(1, 100)
                .onRequest().invoke(() -> System.out.println("-------------"))
                .paceDemand().on(Infrastructure.getDefaultWorkerPool()).using(pacer)
                .subscribe().with(System.out::println, Throwable::printStackTrace, latch::countDown);

        latch.await(1, TimeUnit.MINUTES);
    }
}