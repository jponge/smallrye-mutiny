package io.smallrye.mutiny.operators.multi.replay;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class AppendOnlyReplayListTest {

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
}
