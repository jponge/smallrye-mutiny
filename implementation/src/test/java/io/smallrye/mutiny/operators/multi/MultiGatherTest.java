package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class MultiGatherTest {

    @Test
    void yolo() {
        AssertSubscriber<ArrayList<Integer>> sub = Multi.createFrom().range(1, 100)
                .gather(
                        () -> new ArrayList<Integer>(),
                        (list, next) -> {
                            list.add(next);
                            return list;
                        },
                        list -> list.size() == 10,
                        (list, isCompletion) -> list,
                        list -> list
                )
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        sub.awaitCompletion();
        System.out.println(sub.getItems());
    }
}
