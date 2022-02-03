package io.smallrye.mutiny.operators.multi.builders;

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class MultiReplayPublisherTest {

    @Test
    void yolo() {
        Multi<String> multi = Multi.createFrom().range(1, 10)
                .onItem().transform(Object::toString);

        List<String> init = Arrays.asList("foo", "bar", "baz");

        Multi<String> pipeline = Multi.createBy().replaying()
                .from(multi)
                .atMost(5)
                .cancelAfterLastSubscriberLeaves()
                .withInitialValues(init);

        pipeline.subscribe().with(System.out::println);
        System.out.println("----");
        pipeline.subscribe().with(System.out::println);
        System.out.println("----");
        pipeline.subscribe().with(System.out::println);
    }

    @Test
    void whatz() {
        Multi<Integer> multi = Multi.createFrom().range(1, 3)
                .broadcast().withCancellationAfterLastSubscriberDeparture().toAtLeast(1);
//                .broadcast().toAtLeast(1);

        multi.subscribe().with(System.out::println);
        System.out.println("----");
        multi.subscribe().with(System.out::println);
    }
}