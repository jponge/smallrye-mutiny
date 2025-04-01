package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.tuples.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Optional;

class MultiGatherTest {

    @Test
    void smoke_test() {
        AssertSubscriber<ArrayList<Integer>> sub = Multi.createFrom().range(1, 100)
                .onItem().gather(
                        () -> new ArrayList<Integer>(),
                        (list, next) -> {
                            list.add(next);
                            return list;
                        },
                        list -> {
                            if (list.size() > 5) {
                                return Optional.of(Tuple2.of(new ArrayList<>(), list));
                            } else {
                                return Optional.empty();
                            }
                        },
                        list -> {
                            if (list.isEmpty()) {
                                return Optional.empty();
                            } else {
                                return Optional.of(list);
                            }
                        }
                )
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.awaitNextItems(2);
        System.out.println(sub.getItems());
        sub.request(Long.MAX_VALUE);
        sub.awaitCompletion();
        System.out.println(sub.getItems());
    }

    @Test
    void api_design() {
        AssertSubscriber<ArrayList<Integer>> sub = Multi.createFrom().range(1, 100)
                .onItem().gather()
                    .withAccumulator(ArrayList<Integer>::new)
                    .accumulate((list, next) -> {
                        list.add(next);
                        return list;
                    })
                    .extract(list -> {
                        if (list.size() > 5) {
                            return Optional.of(Tuple2.of(new ArrayList<>(), list));
                        } else {
                            return Optional.empty();
                        }
                    }).finalize(list -> {
                        if (list.isEmpty()) {
                            return Optional.empty();
                        } else {
                            return Optional.of(list);
                        }
                    })
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.awaitNextItems(2);
        System.out.println(sub.getItems());
        sub.request(Long.MAX_VALUE);
        sub.awaitCompletion();
        System.out.println(sub.getItems());
    }
}