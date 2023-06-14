package io.smallrye.mutiny.operators.multi.split;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

class MultiSplitterTest {

    enum OddEven {
        ODD,
        EVEN
    }

    @Test
    void wip() {
        var splitter = Multi.createFrom().items(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .split(OddEven.class, n -> (n % 2 == 0) ? OddEven.EVEN : OddEven.ODD);

        AssertSubscriber<Integer> oddSub = AssertSubscriber.create();
        AssertSubscriber<Integer> evenSub = AssertSubscriber.create();

        splitter.get(OddEven.ODD)
                .onItem().invoke(n -> System.out.println("odd -> " + n))
                .subscribe().withSubscriber(oddSub);

        oddSub.request(2L);

        splitter.get(OddEven.EVEN)
                .onItem().invoke(n -> System.out.println("even -> " + n))
                .subscribe().withSubscriber(evenSub);

        evenSub.request(2L);
        oddSub.request(Long.MAX_VALUE);
    }
}
