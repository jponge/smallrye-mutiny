package io.smallrye.mutiny.operators.multi;

import static io.smallrye.mutiny.helpers.MultiSubscribers.toMultiSubscriber;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.reactivex.rxjava3.processors.PublishProcessor;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.test.Mocks;
import mutiny.zero.flow.adapters.AdaptersToFlow;

public class FlatMapMainSubscriberTest {

    @Test
    public void testThatInvalidRequestAreRejectedByMainSubscriber() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create();

        MultiFlatMapOp.FlatMapMainSubscriber<Integer, Integer> sub = new MultiFlatMapOp.FlatMapMainSubscriber<>(
                toMultiSubscriber(subscriber),
                i -> Multi.createFrom().item(2),
                false,
                4,
                10);

        Multi.createFrom().item(1)
                .subscribe().withSubscriber(sub);

        sub.request(-1);
        subscriber.assertFailedWith(IllegalArgumentException.class, "");
    }

    @Test
    public void testCancellationFromMainSubscriber() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create();

        MultiFlatMapOp.FlatMapMainSubscriber<Integer, Integer> sub = new MultiFlatMapOp.FlatMapMainSubscriber<>(
                toMultiSubscriber(subscriber),
                i -> Multi.createFrom().item(2),
                false,
                4,
                10);

        Multi.createFrom().item(1)
                .subscribe().withSubscriber(sub);

        sub.cancel();
        subscriber.assertNotTerminated();
        sub.cancel();
        sub.onItem(1);
        subscriber.assertHasNotReceivedAnyItem();
    }

    @Test
    public void testThatNoItemsAreDispatchedAfterCompletion() {
        Subscriber<Integer> subscriber = Mocks.subscriber();

        MultiFlatMapOp.FlatMapMainSubscriber<Integer, Integer> sub = new MultiFlatMapOp.FlatMapMainSubscriber<>(
                toMultiSubscriber(subscriber),
                i -> Multi.createFrom().item(2),
                false,
                4,
                10);

        sub.onSubscribe(mock(Subscription.class));
        sub.onNext(1);
        sub.onComplete();

        sub.onNext(2);
        sub.onComplete();

        verify(subscriber).onNext(2);
        verify(subscriber).onComplete();
        verify(subscriber, never()).onError(any(Throwable.class));
    }

    @Test
    public void testThatNoItemsAreDispatchedAfterFailure() {
        Subscriber<Integer> subscriber = Mocks.subscriber();

        MultiFlatMapOp.FlatMapMainSubscriber<Integer, Integer> sub = new MultiFlatMapOp.FlatMapMainSubscriber<>(
                toMultiSubscriber(subscriber),
                i -> Multi.createFrom().item(2),
                false,
                4,
                10);

        sub.onSubscribe(mock(Subscription.class));
        sub.onNext(1);
        sub.onError(new Exception("boom"));

        sub.onNext(2);
        sub.onComplete();
        sub.onError(new Exception("boom"));

        verify(subscriber).onNext(2);
        verify(subscriber, never()).onComplete();
        verify(subscriber).onError(any(Throwable.class));
    }

    @Test
    public void testWhenInnerCompletesAfterOnNextInDrainThenCancels() {
        PublishProcessor<Integer> pp = PublishProcessor.create();
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create();

        Multi.createFrom().item(1)
                .onItem().transformToMulti(v -> AdaptersToFlow.publisher(pp)).merge()
                .onItem().invoke(v -> {
                    if (v == 1) {
                        pp.onComplete();
                        subscriber.cancel();
                    }
                })
                .subscribe().withSubscriber(subscriber);

        pp.onNext(1);

        subscriber.request(1)
                .assertItems(1);
    }

    @Test
    public void testDemandHonoredWithBoundedRequests() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, 20)
                .onItem().transformToMulti(v -> Multi.createFrom().items(v, v))
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.request(5);
        assertThat(subscriber.getItems()).hasSize(5);
        subscriber.assertNotTerminated();

        subscriber.request(5);
        assertThat(subscriber.getItems()).hasSize(10);

        subscriber.request(Long.MAX_VALUE);
        subscriber.awaitCompletion();
        assertThat(subscriber.getItems()).hasSize(40);
    }

    @Test
    public void testDemandWithSlowInnerStreams() {
        CompletableFuture<Integer> future1 = new CompletableFuture<>();
        CompletableFuture<Integer> future2 = new CompletableFuture<>();
        CompletableFuture<Integer> future3 = new CompletableFuture<>();
        List<CompletableFuture<Integer>> futures = List.of(future1, future2, future3);

        AtomicInteger index = new AtomicInteger();
        AssertSubscriber<Integer> subscriber = Multi.createFrom().items(1, 2, 3)
                .onItem().transformToMulti(v -> Uni.createFrom()
                        .completionStage(futures.get(index.getAndIncrement()))
                        .toMulti())
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(2));

        future1.complete(10);
        future2.complete(20);
        assertThat(subscriber.getItems()).hasSize(2);

        subscriber.request(1);
        future3.complete(30);
        subscriber.awaitCompletion();
        assertThat(subscriber.getItems()).hasSize(3);
    }

    @Test
    public void testUpstreamReplenishmentWithConcurrency() {
        AtomicInteger upstreamEmitted = new AtomicInteger();
        AssertSubscriber<Integer> subscriber = Multi.createFrom()
                .<AtomicInteger, Integer> generator(() -> new AtomicInteger(0), (counter, emitter) -> {
                    int val = counter.getAndIncrement();
                    upstreamEmitted.incrementAndGet();
                    if (val < 20) {
                        emitter.emit(val);
                    } else {
                        emitter.complete();
                    }
                    return counter;
                })
                .onItem().transformToMulti(v -> Multi.createFrom().items(v))
                .withRequests(1)
                .merge(4)
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        subscriber.awaitItems(10);
        assertThat(upstreamEmitted.get()).isGreaterThanOrEqualTo(10);
    }

    @RepeatedTest(10)
    public void testConcurrentInnersAllItemsDelivered() {
        var executor = Executors.newFixedThreadPool(8);
        try {
            List<Integer> expected = IntStream.range(0, 100).boxed().collect(Collectors.toList());

            AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, 100)
                    .onItem().transformToMulti(v -> Multi.createFrom().items(v)
                            .emitOn(executor))
                    .merge(16)
                    .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

            subscriber.awaitCompletion(Duration.ofSeconds(10));
            assertThat(subscriber.getItems()).containsExactlyInAnyOrderElementsOf(expected);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConcurrentInnersWithMultipleItemsPerInner() {
        var executor = Executors.newFixedThreadPool(8);
        try {
            AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, 50)
                    .onItem().transformToMulti(v -> Multi.createFrom().items(v * 10, v * 10 + 1, v * 10 + 2)
                            .emitOn(executor))
                    .merge(8)
                    .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

            subscriber.awaitCompletion(Duration.ofSeconds(10));
            assertThat(subscriber.getItems()).hasSize(150);
            assertThat(subscriber.getItems()).doesNotHaveDuplicates();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManyInnersCompletingSimultaneously() throws InterruptedException {
        var executor = Executors.newFixedThreadPool(8);
        try {
            int n = 20;
            List<CompletableFuture<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(new CompletableFuture<>());
            }

            AtomicInteger idx = new AtomicInteger();
            AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, n)
                    .onItem().transformToMulti(v -> Uni.createFrom()
                            .completionStage(futures.get(idx.getAndIncrement()))
                            .toMulti())
                    .merge(n)
                    .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

            CountDownLatch latch = new CountDownLatch(1);
            for (int i = 0; i < n; i++) {
                final int val = i;
                executor.submit(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    futures.get(val).complete(val);
                });
            }
            latch.countDown();

            subscriber.awaitCompletion(Duration.ofSeconds(10));
            List<Integer> expected = IntStream.range(0, n).boxed().collect(Collectors.toList());
            assertThat(subscriber.getItems()).containsExactlyInAnyOrderElementsOf(expected);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFailFastCancelsUpstreamAndInners() {
        AtomicBoolean upstreamCancelled = new AtomicBoolean();
        AtomicBoolean innerCancelled = new AtomicBoolean();

        AssertSubscriber<Integer> subscriber = Multi.createFrom().items(1, 2)
                .onCancellation().invoke(() -> upstreamCancelled.set(true))
                .onItem().transformToMulti(v -> {
                    if (v == 1) {
                        return Multi.createFrom().ticks().every(Duration.ofSeconds(1))
                                .onItem().castTo(Integer.class)
                                .onCancellation().invoke(() -> innerCancelled.set(true));
                    } else {
                        return Multi.createFrom().failure(new RuntimeException("boom"));
                    }
                })
                .merge(2)
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitFailure(t -> assertThat(t).hasMessage("boom"));
        await().atMost(Duration.ofSeconds(5)).untilTrue(innerCancelled);
    }

    @Test
    public void testFailFastFromInnerWithQueuedItems() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);

        Multi.createFrom().items(1, 2)
                .onItem().transformToMulti(v -> {
                    if (v == 1) {
                        return Multi.createFrom().items(10, 11, 12);
                    } else {
                        return Multi.createFrom().<Integer> failure(new RuntimeException("boom"));
                    }
                })
                .merge(2)
                .subscribe().withSubscriber(subscriber);

        subscriber.awaitFailure(t -> assertThat(t).hasMessage("boom"));
        assertThat(subscriber.getItems()).isEmpty();
    }

    @Test
    public void testFailFastFromMapperException() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(1, 6)
                .onItem().transformToMulti(v -> {
                    if (v == 3) {
                        throw new RuntimeException("boom");
                    }
                    return Multi.createFrom().items(v);
                })
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitFailure(t -> assertThat(t).hasMessage("boom"));
        assertThat(subscriber.getItems()).contains(1, 2);
    }

    @Test
    public void testCollectFailuresAllInnersFailAndUpstreamCompletes() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(1, 6)
                .onItem().transformToMulti(v -> Multi.createFrom()
                        .<Integer> failure(new RuntimeException("fail-" + v)))
                .collectFailures()
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitFailure(t -> {
            assertThat(t).isInstanceOf(CompositeException.class);
            CompositeException ce = (CompositeException) t;
            assertThat(ce.getCauses()).hasSize(5);
            for (int i = 0; i < 5; i++) {
                final int idx = i + 1;
                assertThat(ce.getCauses()).anyMatch(c -> c.getMessage().equals("fail-" + idx));
            }
        });
    }

    @Test
    public void testCollectFailuresMixOfSuccessAndFailure() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, 10)
                .onItem().transformToMulti(v -> {
                    if (v % 2 == 0) {
                        return Multi.createFrom().items(v);
                    } else {
                        return Multi.createFrom().<Integer> failure(new RuntimeException("fail-" + v));
                    }
                })
                .collectFailures()
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitFailure(t -> {
            assertThat(t).isInstanceOf(CompositeException.class);
            CompositeException ce = (CompositeException) t;
            assertThat(ce.getCauses()).hasSize(5);
        });
        assertThat(subscriber.getItems()).containsExactlyInAnyOrder(0, 2, 4, 6, 8);
    }

    @Test
    public void testCollectFailuresContinuesAfterInnerError() {
        AtomicBoolean upstreamCancelled = new AtomicBoolean();

        PublishProcessor<Integer> upstream = PublishProcessor.create();
        PublishProcessor<Integer> inner1 = PublishProcessor.create();
        PublishProcessor<Integer> inner2 = PublishProcessor.create();

        AtomicInteger innerIdx = new AtomicInteger();
        List<PublishProcessor<Integer>> innerProcessors = List.of(inner1, inner2);

        AssertSubscriber<Integer> subscriber = Multi.createFrom().publisher(AdaptersToFlow.publisher(upstream))
                .onCancellation().invoke(() -> upstreamCancelled.set(true))
                .onItem().transformToMulti(v -> Multi.createFrom().publisher(
                        AdaptersToFlow.publisher(innerProcessors.get(innerIdx.getAndIncrement()))))
                .collectFailures()
                .merge(10)
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        upstream.onNext(1);
        inner1.onError(new RuntimeException("inner-1-fail"));

        subscriber.assertNotTerminated();
        assertThat(upstreamCancelled.get()).isFalse();

        upstream.onNext(2);
        inner2.onNext(42);
        assertThat(subscriber.getItems()).contains(42);

        inner2.onComplete();
        upstream.onComplete();

        subscriber.awaitFailure(t -> {
            assertThat(t).isInstanceOf(RuntimeException.class);
            assertThat(t).hasMessage("inner-1-fail");
        });
    }

    @Test
    public void testCancelDuringDrainClearsQueue() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create(0);

        Multi.createFrom().range(0, 10)
                .onItem().transformToMulti(v -> Multi.createFrom().items(v))
                .merge()
                .subscribe().withSubscriber(subscriber);

        subscriber.cancel();
        assertThat(subscriber.getItems()).isEmpty();
        subscriber.assertNotTerminated();
    }

    @Test
    public void testCancelPropagatesToAllActiveInners() {
        int innerCount = 4;
        List<AtomicBoolean> innerCancellations = new ArrayList<>();
        for (int i = 0; i < innerCount; i++) {
            innerCancellations.add(new AtomicBoolean());
        }

        AtomicInteger idx = new AtomicInteger();
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, innerCount)
                .onItem().transformToMulti(v -> {
                    int myIdx = idx.getAndIncrement();
                    return Multi.createFrom().<Integer> nothing()
                            .onCancellation().invoke(() -> innerCancellations.get(myIdx).set(true));
                })
                .merge(innerCount)
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.cancel();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            for (int i = 0; i < innerCount; i++) {
                assertThat(innerCancellations.get(i).get())
                        .as("inner %d should be cancelled", i)
                        .isTrue();
            }
        });
    }

    @RepeatedTest(100)
    public void testCancelVsInnerCompletionRace() {
        PublishProcessor<Integer> inner = PublishProcessor.create();

        AssertSubscriber<Integer> subscriber = Multi.createFrom().item(1)
                .onItem().transformToMulti(v -> Multi.createFrom().publisher(AdaptersToFlow.publisher(inner)))
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        CountDownLatch start = new CountDownLatch(1);

        Thread completer = new Thread(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inner.onNext(1);
            inner.onComplete();
        });

        Thread canceller = new Thread(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            subscriber.cancel();
        });

        completer.start();
        canceller.start();
        start.countDown();

        try {
            completer.join(5000);
            canceller.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }

    @Test
    public void testEmptyInnerStreams() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, 5)
                .onItem().transformToMulti(v -> Multi.createFrom().<Integer> empty())
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitCompletion();
        assertThat(subscriber.getItems()).isEmpty();
    }

    @Test
    public void testMixOfEmptyAndNonEmptyInners() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().range(0, 10)
                .onItem().transformToMulti(v -> {
                    if (v % 2 == 0) {
                        return Multi.createFrom().<Integer> empty();
                    } else {
                        return Multi.createFrom().items(v);
                    }
                })
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitCompletion();
        assertThat(subscriber.getItems()).containsExactlyInAnyOrder(1, 3, 5, 7, 9);
    }

    @Test
    public void testSingleItemWithUnboundedDemand() {
        AssertSubscriber<Integer> subscriber = Multi.createFrom().item(42)
                .onItem().transformToMulti(v -> Multi.createFrom().items(v))
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.awaitCompletion();
        assertThat(subscriber.getItems()).containsExactly(42);
    }

    @Test
    public void testUpstreamCompletionWaitsForActiveInners() {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        AssertSubscriber<Integer> subscriber = Multi.createFrom().item(1)
                .onItem().transformToMulti(v -> Uni.createFrom()
                        .completionStage(future)
                        .toMulti())
                .merge()
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        subscriber.assertNotTerminated();
        assertThat(subscriber.getItems()).isEmpty();

        future.complete(99);

        subscriber.awaitCompletion();
        assertThat(subscriber.getItems()).containsExactly(99);
    }

    @Test
    public void testDoubleOnCompletionIsIgnored() {
        AssertSubscriber<Integer> subscriber = AssertSubscriber.create(Long.MAX_VALUE);

        MultiFlatMapOp.FlatMapMainSubscriber<Integer, Integer> sub = new MultiFlatMapOp.FlatMapMainSubscriber<>(
                toMultiSubscriber(subscriber),
                i -> Multi.createFrom().item(i),
                false,
                4,
                10);

        sub.onSubscribe(mock(Subscription.class));
        sub.onComplete();
        sub.onComplete();

        subscriber.assertCompleted();
        subscriber.assertHasNotReceivedAnyItem();
    }
}
