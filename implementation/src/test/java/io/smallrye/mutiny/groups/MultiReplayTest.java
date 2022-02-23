package io.smallrye.mutiny.groups;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

class MultiReplayTest {

    @Test
    void basicReplayAll() {
        Multi<Integer> upstream = Multi.createFrom().range(1, 10);
        Multi<Integer> replay = Multi.createBy().replaying().ofMulti(upstream);

        AssertSubscriber<Integer> sub = replay.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(Long.MAX_VALUE);
        assertThat(sub.getItems()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
        sub.assertCompleted();

        sub = replay.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(4);
        assertThat(sub.getItems()).containsExactly(1, 2, 3, 4);
        sub.request(Long.MAX_VALUE);
        assertThat(sub.getItems()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
        sub.assertCompleted();
    }

    @Test
    void basicReplayLatest3() {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        AtomicBoolean step = new AtomicBoolean();

        Multi<Integer> upstream = Multi.createFrom().<Integer> emitter(emitter -> {
            await().untilTrue(step);
            for (int i = 0; i <= 10; i++) {
                emitter.emit(i);
            }
            emitter.complete();
        }).runSubscriptionOn(pool);
        Multi<Integer> replay = Multi.createBy().replaying().upTo(3).ofMulti(upstream);

        AssertSubscriber<Integer> sub = replay.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        step.set(true);
        sub.awaitCompletion();
        assertThat(sub.getItems()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        sub = replay.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(1);
        assertThat(sub.getItems()).containsExactly(8);
        sub.request(1);
        assertThat(sub.getItems()).containsExactly(8, 9);
        sub.request(3000);
        assertThat(sub.getItems()).containsExactly(8, 9, 10);
        sub.assertCompleted();
    }

    @Test
    void replayLast3AfterFailure() {
        Multi<Integer> upstream = Multi.createBy().concatenating().streams(
                Multi.createFrom().range(1, 10),
                Multi.createFrom().failure(() -> new IOException("boom")));
        Multi<Integer> replay = Multi.createBy().replaying().upTo(3).ofMulti(upstream);

        AssertSubscriber<Integer> sub = replay.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        sub.assertFailedWith(IOException.class, "boom");
        sub.assertItems(7, 8, 9);

        sub = replay.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        sub.assertFailedWith(IOException.class, "boom");
        sub.assertItems(7, 8, 9);
    }

    @Test
    void replayWithSeed() {
        List<Integer> seed = Arrays.asList(-100, -10, -1);
        Multi<Integer> upstream = Multi.createFrom().range(0, 11);
        Multi<Integer> replay = Multi.createBy().replaying().ofMultiWithSeed(upstream, seed);

        AssertSubscriber<Integer> sub = replay.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        sub.assertCompleted();
        assertThat(sub.getItems()).containsExactly(-100, -10, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
