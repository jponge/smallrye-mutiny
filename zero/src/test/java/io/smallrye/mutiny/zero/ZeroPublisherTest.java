package io.smallrye.mutiny.zero;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;

class ZeroPublisherTest {

    @Nested
    @DisplayName("Publisher from iterables")
    class Iterables {

        @Test
        @DisplayName("Items from a null collection")
        void fromNull() {
            Object[] array = null;
            Assertions.assertThrows(NullPointerException.class, () -> ZeroPublisher.fromItems(array));

            List<?> collection = null;
            Assertions.assertThrows(NullPointerException.class, () -> ZeroPublisher.fromIterable(collection));
        }

        @Test
        @DisplayName("Items from a collection (request batches)")
        void fromItemsInBatches() {
            AssertSubscriber<Object> sub = AssertSubscriber.create();
            ZeroPublisher.fromItems(1, 2, 3).subscribe(sub);

            sub.assertNotTerminated();
            sub.request(1);
            sub.assertItems(1);
            sub.assertNotTerminated();
            sub.request(10);
            sub.assertItems(1, 2, 3);
            sub.assertCompleted();
        }

        @Test
        @DisplayName("Items from a collection (unbounded initial request)")
        void fromItemsUnbounded() {
            AssertSubscriber<Object> sub = AssertSubscriber.create(Long.MAX_VALUE);
            ZeroPublisher.fromItems(1, 2, 3).subscribe(sub);

            sub.assertItems(1, 2, 3);
            sub.assertCompleted();
        }

        @Test
        @DisplayName("Items from a collection (midway cancellation)")
        void fromItemsCancellation() {
            AssertSubscriber<Object> sub = AssertSubscriber.create();
            ZeroPublisher.fromItems(1, 2, 3).subscribe(sub);

            sub.assertNotTerminated();
            sub.request(1);
            sub.assertItems(1);

            sub.cancel();
            sub.assertNotTerminated();
        }

        @Test
        @DisplayName("Items from a collection (unbounded initial request, presence of a null value)")
        void fromItemsUnboundedWithNull() {
            AssertSubscriber<Object> sub = AssertSubscriber.create(Long.MAX_VALUE);
            ZeroPublisher.fromItems(1, null, 3).subscribe(sub);

            sub.assertFailedWith(NullPointerException.class, "null value");
        }
    }

    @Nested
    @DisplayName("Publisher from CompletionStage")
    class CompletionStages {

        @Test
        @DisplayName("Null CompletionStage")
        void fromNull() {
            Assertions.assertThrows(NullPointerException.class, () -> ZeroPublisher.fromCompletionStage(null));
        }

        @Test
        @DisplayName("Resolved CompletionStage (value)")
        void fromResolvedValue() {
            CompletableFuture<Integer> future = CompletableFuture.completedFuture(58);
            AssertSubscriber<Object> sub = AssertSubscriber.create(10);
            ZeroPublisher.fromCompletionStage(future).subscribe(sub);

            sub.assertCompleted();
            sub.assertItems(58);
        }

        @Test
        @DisplayName("Resolved CompletionStage (error)")
        void fromResolvedFailure() {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("boom"));
            AssertSubscriber<Object> sub = AssertSubscriber.create(10);
            ZeroPublisher.fromCompletionStage(future).subscribe(sub);

            sub.assertFailedWith(RuntimeException.class, "boom");
        }

        @Test
        @DisplayName("Deferred CompletionStage (value)")
        void fromDeferredValue() {
            CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return 63;
            });
            AssertSubscriber<Object> sub = AssertSubscriber.create(10);
            ZeroPublisher.fromCompletionStage(future).subscribe(sub);

            sub.awaitNextItem();
            sub.assertItems(63);
            sub.assertCompleted();
        }

        @Test
        @DisplayName("Resolved CompletionStage (null value)")
        void fromResolvedNullValue() {
            CompletableFuture<Object> future = CompletableFuture.completedFuture(null);
            AssertSubscriber<Object> sub = AssertSubscriber.create(10);
            ZeroPublisher.fromCompletionStage(future).subscribe(sub);

            sub.assertFailedWith(NullPointerException.class, "null value");
        }

        @Test
        @DisplayName("Deferred CompletionStage (null value)")
        void fromDeferredNullValue() {
            CompletableFuture<Object> future = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return null;
            });
            AssertSubscriber<Object> sub = AssertSubscriber.create(10);
            ZeroPublisher.fromCompletionStage(future).subscribe(sub);

            sub.awaitFailure();
            sub.assertFailedWith(NullPointerException.class, "null value");
        }
    }
}
