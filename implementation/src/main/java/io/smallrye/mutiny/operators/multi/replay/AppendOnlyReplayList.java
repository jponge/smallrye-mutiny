package io.smallrye.mutiny.operators.multi.replay;

import java.util.function.BiPredicate;

/*
 * Replay is being captured using a custom linked list, while consumers can make progress using cursors.
 * Cursors need to be made ready by calling `readyAtStart()`, which would typically be done at subscription time.
 *
 * The "start" depends on the replay semantics:
 * - zero for unbounded replays,
 * - the last n elements before the tail for bounded replays.
 *
 * From there each cursor (1 per subscriber) can make progress at its own pace.
 *
 * The code assumes reactive streams semantics, especially that there are no concurrent appends because of
 * serial events.
 *
 * Bounded replays shall have earlier cells before the head be eventually garbage collected as there are only forward
 * references.
 */
public class AppendOnlyReplayList {

    public class Cursor {

        private volatile Cell current = SENTINEL_EMPTY;

        public boolean readyAtStart() {
            if (current == SENTINEL_EMPTY) {
                if (head != SENTINEL_EMPTY) {
                    current = head;
                    return true;
                }
            }
            return false;
        }

        public boolean canMoveForward() {
            return current != SENTINEL_EMPTY && current.next != SENTINEL_END;
        }

        public void moveForward() {
            if (canMoveForward()) {
                current = current.next;
            }
        }

        public Object unwrap() {
            return current.value;
        }

        public boolean hasReachedCompletion() {
            return current.value instanceof Completion;
        }

        public boolean hasReachedFailure() {
            return current.value instanceof Failure;
        }

        public Throwable unwrapFailure() {
            return ((Failure) current.value).failure;
        }
    }

    private abstract class Terminal {

    }

    private final class Completion extends Terminal {

        @Override
        public String toString() {
            return "[completion]";
        }
    }

    private final class Failure extends Terminal {
        Throwable failure;

        Failure(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public String toString() {
            return "[failure] " + failure.getMessage();
        }
    }

    private static class Cell {
        Object value;
        volatile Cell next;

        Cell(Object value, Cell next) {
            this.value = value;
            this.next = next;
        }
    }

    private static final Cell SENTINEL_END = new Cell(null, null);
    private static final Cell SENTINEL_EMPTY = new Cell(null, SENTINEL_END);

    private final long itemsToReplay;
    private long numberOfItemsRecorded = 0L;
    private volatile Cell head = SENTINEL_EMPTY;
    private volatile Cell tail = SENTINEL_EMPTY;

    public AppendOnlyReplayList(long itemsToReplay) {
        assert itemsToReplay > 0;
        this.itemsToReplay = itemsToReplay;
    }

    public void push(Object item) {
        assert !(tail.value instanceof Terminal);
        Cell newCell = new Cell(item, SENTINEL_END);
        if (head == SENTINEL_EMPTY) {
            head = newCell;
        } else {
            tail.next = newCell;
        }
        tail = newCell;
        if (itemsToReplay != Long.MAX_VALUE && !(item instanceof Terminal)) {
            numberOfItemsRecorded++;
            if (numberOfItemsRecorded > itemsToReplay) {
                head = head.next;
            }
        }
    }

    public void pushFailure(Throwable failure) {
        push(new Failure(failure));
    }

    public void pushCompletion() {
        push(new Completion());
    }

    public Cursor newCursor() {
        return new Cursor();
    }

    void runSanityCheck(BiPredicate<Object, Object> predicate) {
        if (head == tail) {
            return;
        }
        Cell last = head;
        Cell current = last.next;
        while (current != tail) {
            if (!predicate.test(last.value, current.value)) {
                throw new IllegalStateException(last.value + " vs " + current.value);
            }
            last = current;
            current = current.next;
        }
    }
}
