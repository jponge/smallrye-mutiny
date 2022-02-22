package io.smallrye.mutiny.operators.multi.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class AppendOnlyReplayListTest {

    private Random random = new Random();

    @Test
    void checkReadyAtStart() {
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(Long.MAX_VALUE);
        AppendOnlyReplayList.Cursor cursor = replayList.newCursor();

        assertThat(cursor.readyAtStart()).isFalse();
        replayList.push("foo");
        replayList.push("bar");
        assertThat(cursor.readyAtStart()).isTrue();
        assertThat(cursor.unwrap()).isEqualTo("foo");
        assertThat(cursor.canMoveForward()).isTrue();
        cursor.moveForward();
        assertThat(cursor.readyAtStart()).isTrue();
        assertThat(cursor.unwrap()).isEqualTo("bar");
        assertThat(cursor.canMoveForward()).isFalse();
    }

    @Test
    void pushSomeItemsAndComplete() {
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(Long.MAX_VALUE);
        ArrayList<Integer> reference = new ArrayList<>();

        AppendOnlyReplayList.Cursor firstCursor = replayList.newCursor();
        assertThat(firstCursor.readyAtStart()).isFalse();
        for (int i = 0; i < 20; i++) {
            replayList.push(i);
            reference.add(i);
        }
        replayList.pushCompletion();
        AppendOnlyReplayList.Cursor secondCursor = replayList.newCursor();

        checkCompletedWithAllItems(reference, firstCursor);
        checkCompletedWithAllItems(reference, secondCursor);
    }

    private void checkCompletedWithAllItems(ArrayList<Integer> reference, AppendOnlyReplayList.Cursor cursor) {
        ArrayList<Integer> proof = new ArrayList<>();
        assertThat(cursor.readyAtStart()).isTrue();
        while (true) {
            if (cursor.hasReachedCompletion()) {
                break;
            }
            Assumptions.assumeFalse(cursor.hasReachedFailure());
            proof.add((Integer) cursor.unwrap());
            if (cursor.canMoveForward()) {
                cursor.moveForward();
            } else {
                break;
            }
        }
        assertThat(proof).isEqualTo(reference);
    }

    @Test
    void pushSomeItemsAndFail() {
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(Long.MAX_VALUE);
        ArrayList<Integer> reference = new ArrayList<>();

        AppendOnlyReplayList.Cursor firstCursor = replayList.newCursor();
        for (int i = 0; i < 20; i++) {
            replayList.push(i);
            reference.add(i);
        }
        replayList.pushFailure(new IOException("woops"));
        AppendOnlyReplayList.Cursor secondCursor = replayList.newCursor();

        checkFailedWithAllItems(reference, firstCursor, IOException.class, "woops");
        checkFailedWithAllItems(reference, secondCursor, IOException.class, "woops");
    }

    private void checkFailedWithAllItems(ArrayList<Integer> reference, AppendOnlyReplayList.Cursor cursor, Class<?> failureType,
            String failureMessage) {
        ArrayList<Integer> proof = new ArrayList<>();
        assertThat(cursor.readyAtStart()).isTrue();
        while (true) {
            if (cursor.hasReachedFailure()) {
                break;
            }
            Assumptions.assumeFalse(cursor.hasReachedCompletion());
            proof.add((Integer) cursor.unwrap());
            if (cursor.canMoveForward()) {
                cursor.moveForward();
            } else {
                break;
            }
        }
        assertThat(proof).isEqualTo(reference);
        assertThat(cursor.hasReachedFailure()).isTrue();
        assertThat(cursor.unwrapFailure()).isInstanceOf(failureType).hasMessage(failureMessage);
    }

    @Test
    void boundedReplay() {
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(3);
        replayList.push(1);
        replayList.push(2);

        AppendOnlyReplayList.Cursor firstCursor = replayList.newCursor();
        assertThat(firstCursor.readyAtStart()).isTrue();
        assertThat(firstCursor.unwrap()).isEqualTo(1);
        assertThat(firstCursor.canMoveForward()).isTrue();
        firstCursor.moveForward();
        assertThat(firstCursor.unwrap()).isEqualTo(2);
        assertThat(firstCursor.canMoveForward()).isFalse();

        AppendOnlyReplayList.Cursor secondCursor = replayList.newCursor();
        replayList.push(3);
        replayList.push(4);
        replayList.push(5);

        assertThat(secondCursor.readyAtStart()).isTrue();
        assertThat(secondCursor.unwrap()).isEqualTo(3);
        secondCursor.moveForward();
        assertThat(secondCursor.unwrap()).isEqualTo(4);
        secondCursor.moveForward();
        assertThat(secondCursor.unwrap()).isEqualTo(5);
        secondCursor.moveForward();
        assertThat(secondCursor.canMoveForward()).isFalse();

        assertThat(firstCursor.canMoveForward()).isTrue();
        firstCursor.moveForward();
        assertThat(firstCursor.unwrap()).isEqualTo(3);
        firstCursor.moveForward();
        assertThat(firstCursor.unwrap()).isEqualTo(4);

        replayList.push(6);
        replayList.pushFailure(new IOException("boom"));

        AppendOnlyReplayList.Cursor lateCursor = replayList.newCursor();
        assertThat(lateCursor.readyAtStart()).isTrue();
        assertThat(lateCursor.unwrap()).isEqualTo(4);
        assertThat(lateCursor.canMoveForward()).isTrue();
        lateCursor.moveForward();
        assertThat(lateCursor.unwrap()).isEqualTo(5);
        assertThat(lateCursor.canMoveForward()).isTrue();
        lateCursor.moveForward();
        assertThat(lateCursor.unwrap()).isEqualTo(6);
        assertThat(lateCursor.canMoveForward()).isTrue();
        lateCursor.moveForward();
        assertThat(lateCursor.hasReachedFailure()).isTrue();
        assertThat(lateCursor.canMoveForward()).isFalse();
        assertThat(lateCursor.unwrapFailure()).isInstanceOf(IOException.class).hasMessage("boom");
    }

    @Test
    void runSanityChecksOnChainingBounded() {
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(256);
        for (int i = 0; i < 100_000; i++) {
            replayList.push(i);
        }
        replayList.runSanityCheck((prevRef, nextRef) -> {
            int prev = (int) prevRef;
            int next = (int) nextRef;
            return (prev + 1) == next;
        });
    }

    @Test
    void runSanityChecksOnChainingUnbounded() {
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(Long.MAX_VALUE);
        for (int i = 0; i < 100_000; i++) {
            replayList.push(i);
        }
        replayList.runSanityCheck((prevRef, nextRef) -> {
            int prev = (int) prevRef;
            int next = (int) nextRef;
            return (prev + 1) == next;
        });
    }

    @Test
    void concurrencySanityChecks() {
        final int N_CONSUMERS = 4;
        AppendOnlyReplayList replayList = new AppendOnlyReplayList(256);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong counter = new AtomicLong();
        ConcurrentLinkedDeque<String> problems = new ConcurrentLinkedDeque<>();
        ExecutorService pool = Executors.newCachedThreadPool();

        pool.submit(() -> {
            randomSleep();
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 5000L) {
                for (int i = 0; i < 5000; i++) {
                    replayList.push(counter.getAndIncrement());
                }
            }
            replayList.runSanityCheck((a, b) -> {
                long x = (long) a;
                long y = (long) b;
                return (x + 1) == y;
            });
            stop.set(true);
        });

        for (int i = 0; i < N_CONSUMERS; i++) {
            pool.submit(() -> {
                AppendOnlyReplayList.Cursor cursor = replayList.newCursor();
                randomSleep();
                while (!cursor.readyAtStart()) {
                    // await
                }
                long previous = (long) cursor.unwrap();
                while (!cursor.canMoveForward()) {
                    // await
                }
                while (!stop.get()) {
                    cursor.moveForward();
                    long current = (long) cursor.unwrap();
                    if (current != previous + 1) {
                        problems.add("Broken sequence " + previous + " -> " + current);
                        return;
                    }
                    previous = current;
                    while (!cursor.canMoveForward() && !stop.get()) {
                        // await
                    }
                }
            });
        }

        await().untilTrue(stop);
        pool.shutdownNow();
        for (String problem : problems) {
            System.out.println(problem);
        }
    }

    private void randomSleep() {
        try {
            Thread.sleep(250 + random.nextInt(250));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
