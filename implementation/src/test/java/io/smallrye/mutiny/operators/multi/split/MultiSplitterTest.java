package io.smallrye.mutiny.operators.multi.split;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;

class MultiSplitterTest {

    enum OddEven {
        ODD,
        EVEN
    }

    @Test
    void wip() {
        var splitter = Multi.createFrom().items(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .split(OddEven.class, n -> (n % 2 == 0) ? OddEven.EVEN : OddEven.ODD);

        splitter.get(OddEven.ODD)
                .subscribe().with(n -> System.out.println("odd -> " + n));

        splitter.get(OddEven.EVEN)
                .subscribe().with(n -> System.out.println("even -> " + n));
    }
}
