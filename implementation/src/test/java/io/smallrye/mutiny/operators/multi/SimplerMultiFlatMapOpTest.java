package io.smallrye.mutiny.operators.multi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;

class SimplerMultiFlatMapOpTest {

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, Integer.MAX_VALUE })
    void simple_steps_over_range(int concurrency) {
        Multi<Integer> multi = Multi.createFrom().range(1, 4);
        SimplerMultiFlatMapOp<Integer, Integer> flatMap = new SimplerMultiFlatMapOp<>(
                multi,
                n -> Multi.createFrom().items(n, n * 10, n * 100),
                false,
                concurrency,
                2);

        AssertSubscriber<Integer> sub = flatMap.subscribe().withSubscriber(AssertSubscriber.create());

        sub.request(1L);
        sub.assertNotTerminated();
        assertThat(sub.getItems()).containsExactly(1);

        sub.request(3L);
        sub.assertNotTerminated();
        assertThat(sub.getItems()).containsExactly(1, 10, 100, 2);

        sub.request(Long.MAX_VALUE);
        sub.assertCompleted();
        assertThat(sub.getItems()).containsExactly(1, 10, 100, 2, 20, 200, 3, 30, 300);
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @CsvSource({ "1, true", "2, false" })
    void simple_steps_over_range_with_interleaving(int concurrency, boolean ordered) {
        Multi<Integer> multi = Multi.createFrom().range(1, 10);
        SimplerMultiFlatMapOp<Integer, Integer> flatMap = new SimplerMultiFlatMapOp<>(
                multi,
                n -> Multi.createFrom().<Integer> emitter(emitter -> {
                    emitter.emit(n);
                    sleep(50 - n);
                    emitter.emit(n * 10);
                    sleep(50 + n);
                    emitter.emit(n * 100);
                    sleep(50 - n);
                    emitter.complete();
                }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()),
                false,
                concurrency,
                2);

        AssertSubscriber<Integer> sub = flatMap.subscribe().withSubscriber(AssertSubscriber.create());

        sub.request(Long.MAX_VALUE).awaitCompletion();
        if (ordered) {
            assertThat(sub.getItems())
                    .hasSize(27)
                    .startsWith(1, 10, 100, 2, 20, 200)
                    .endsWith(9, 90, 900);
        } else {
            assertThat(sub.getItems())
                    .hasSize(27)
                    .contains(900, 100, 1, 20, 3, 9, 90);
        }
    }
}
