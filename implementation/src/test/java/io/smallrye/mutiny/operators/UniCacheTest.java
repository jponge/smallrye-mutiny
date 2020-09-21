package io.smallrye.mutiny.operators;

import static java.util.function.Predicate.isEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;

public class UniCacheTest {

    private static void race(Runnable candidate1, Runnable candidate2, Executor s) {
        final CountDownLatch latch = new CountDownLatch(2);

        final RuntimeException[] errors = { null, null };

        List<Runnable> runnables = new ArrayList<>();
        runnables.add(candidate1);
        runnables.add(candidate2);
        Collections.shuffle(runnables);

        s.execute(() -> {
            try {
                runnables.get(0).run();
            } catch (RuntimeException ex) {
                errors[0] = ex;
            } finally {
                latch.countDown();
            }
        });

        s.execute(() -> {
            try {
                runnables.get(1).run();
            } catch (RuntimeException ex) {
                errors[1] = ex;
            } finally {
                latch.countDown();
            }
        });

        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("The wait timed out!");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }

        if (errors[0] != null) {
            throw errors[0];
        }

        if (errors[1] != null) {
            throw errors[1];
        }
    }

    @Test
    public void testThatSourceCannotBeNull() {
        assertThrows(IllegalArgumentException.class, () -> new UniCache<>(null));
    }

    @Test
    public void testThatImmediateValueAreCached() {
        AtomicInteger counter = new AtomicInteger();
        Uni<Integer> cache = Uni.createFrom().item(counter.incrementAndGet()).cache();

        UniAssertSubscriber<Integer> sub1 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub2 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub3 = UniAssertSubscriber.create();

        cache.subscribe().withSubscriber(sub1);
        cache.subscribe().withSubscriber(sub2);
        cache.subscribe().withSubscriber(sub3);

        sub1.assertCompletedSuccessfully().assertItem(1);
        sub2.assertCompletedSuccessfully().assertItem(1);
        sub3.assertCompletedSuccessfully().assertItem(1);
    }

    @Test
    public void testThatIFailureAreCached() {
        AtomicInteger counter = new AtomicInteger();
        Uni<Object> cache = Uni.createFrom().failure(new Exception("" + counter.getAndIncrement())).cache();

        UniAssertSubscriber<Object> sub1 = UniAssertSubscriber.create();
        UniAssertSubscriber<Object> sub2 = UniAssertSubscriber.create();
        UniAssertSubscriber<Object> sub3 = UniAssertSubscriber.create();

        cache.subscribe().withSubscriber(sub1);
        cache.subscribe().withSubscriber(sub2);
        cache.subscribe().withSubscriber(sub3);

        sub1.assertFailure(Exception.class, "0");
        sub2.assertFailure(Exception.class, "0");
        sub3.assertFailure(Exception.class, "0");
    }

    @Test
    public void testThatValueEmittedAfterSubscriptionAreCached() {
        CompletableFuture<Integer> cs = new CompletableFuture<>();
        Uni<Integer> cache = Uni.createFrom().completionStage(cs).cache();

        UniAssertSubscriber<Integer> sub1 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub2 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub3 = UniAssertSubscriber.create();

        cache.subscribe().withSubscriber(sub1);
        cache.subscribe().withSubscriber(sub2);

        cs.complete(1);

        cache.subscribe().withSubscriber(sub3);

        sub1.assertCompletedSuccessfully().assertItem(1);
        sub2.assertCompletedSuccessfully().assertItem(1);
        sub3.assertCompletedSuccessfully().assertItem(1);
    }

    @Test
    public void testThatSubscriberCanCancelTheirSubscriptionBeforeReceivingAValue() {
        CompletableFuture<Integer> cs = new CompletableFuture<>();
        Uni<Integer> cache = Uni.createFrom().completionStage(cs).cache();

        UniAssertSubscriber<Integer> sub1 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub2 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub3 = UniAssertSubscriber.create();

        cache.subscribe().withSubscriber(sub1);
        cache.subscribe().withSubscriber(sub2);

        sub2.cancel();

        cs.complete(1);

        cache.subscribe().withSubscriber(sub3);

        sub1.assertCompletedSuccessfully().assertItem(1);
        sub2.assertNotCompleted();
        sub3.assertCompletedSuccessfully().assertItem(1);
    }

    @Test
    public void testThatSubscriberCanCancelTheirSubscriptionAfterHavingReceivingAValue() {
        CompletableFuture<Integer> cs = new CompletableFuture<>();
        Uni<Integer> cache = Uni.createFrom().completionStage(cs).cache();

        UniAssertSubscriber<Integer> sub1 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub2 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> sub3 = UniAssertSubscriber.create();

        cache.subscribe().withSubscriber(sub1);
        cache.subscribe().withSubscriber(sub2);

        cs.complete(1);
        sub2.cancel();

        cache.subscribe().withSubscriber(sub3);

        sub1.assertCompletedSuccessfully().assertItem(1);
        sub2.assertCompletedSuccessfully().assertItem(1);
        sub3.assertCompletedSuccessfully().assertItem(1);
    }

    @Test
    public void assertCachingTheValueEmittedByAProcessor() {
        UnicastProcessor<Integer> processor = UnicastProcessor.create();
        Uni<Integer> cached = Uni.createFrom().publisher(processor).cache();

        UniAssertSubscriber<Integer> sub1 = new UniAssertSubscriber<>();
        UniAssertSubscriber<Integer> sub2 = new UniAssertSubscriber<>();

        cached.subscribe().withSubscriber(sub1);
        cached.subscribe().withSubscriber(sub2);

        sub1.assertNotCompleted();
        sub2.assertNotCompleted();

        processor.onNext(23);
        processor.onNext(42);
        processor.onComplete();

        sub1.assertCompletedSuccessfully().assertItem(23);
        sub2.assertCompletedSuccessfully().assertItem(23);
    }

    @Test
    public void assertCancellingImmediately() {
        UnicastProcessor<Integer> processor = UnicastProcessor.create();
        Uni<Integer> cached = Uni.createFrom().publisher(processor).cache();

        UniAssertSubscriber<Integer> sub1 = new UniAssertSubscriber<>(true);
        UniAssertSubscriber<Integer> sub2 = new UniAssertSubscriber<>(true);

        cached.subscribe().withSubscriber(sub1);
        cached.subscribe().withSubscriber(sub2);

        sub1.assertNoResult().assertNoFailure();
        sub2.assertNoResult().assertNoFailure();

        processor.onNext(23);
        processor.onNext(42);
        processor.onComplete();

        sub1.assertNoResult().assertNoFailure();
        sub2.assertNoResult().assertNoFailure();
    }

    @Test
    public void testSubscribersRace() {
        for (int i = 0; i < 2000; i++) {
            Flowable<Integer> flowable = Flowable.just(1, 2, 3);
            Uni<Integer> cached = Uni.createFrom().publisher(flowable).cache();

            UniAssertSubscriber<Integer> subscriber = new UniAssertSubscriber<>(false);

            Runnable r1 = () -> {
                cached.subscribe().withSubscriber(subscriber);
                subscriber.cancel();
            };

            Runnable r2 = () -> cached.subscribe().withSubscriber(new UniAssertSubscriber<>());

            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            try {
                race(r1, r2, executor);
            } finally {
                executor.shutdown();
            }
        }
    }

    @Test
    public void testWithDoubleCancellation() {
        Uni<Integer> uni = Uni.createFrom().item(23).cache();
        UniSubscriber<Integer> subscriber = new UniSubscriber<Integer>() {
            @Override
            public void onSubscribe(UniSubscription subscription) {
                subscription.cancel();
                subscription.cancel();
            }

            @Override
            public void onItem(Integer ignored) {

            }

            @Override
            public void onFailure(Throwable ignored) {

            }
        };
        uni.subscribe().withSubscriber(subscriber);

        UniAssertSubscriber<Integer> test = UniAssertSubscriber.create();
        uni.subscribe().withSubscriber(test);
        test.assertCompletedSuccessfully().assertItem(23);

        uni.subscribe().withSubscriber(subscriber);
    }

    @Test
    public void anotherSubscriberRace() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(32);
        Uni<Integer> uni = Uni.createFrom().deferred(() -> Uni.createFrom().item(0)).cache();

        ConcurrentLinkedDeque<UniAssertSubscriber<Integer>> subscribers = new ConcurrentLinkedDeque<>();
        AtomicInteger counter = new AtomicInteger();
        Runnable work = () -> {
            UniAssertSubscriber<Integer> subscriber = new UniAssertSubscriber<>(false);
            subscribers.add(subscriber);
            uni.onItem().invoke(counter::incrementAndGet).subscribe().withSubscriber(subscriber);
        };
        for (int i = 0; i < 10_000; i++) {
            pool.execute(work);
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(subscribers).hasSize(10_000);
        subscribers.forEach(UniAssertSubscriber::assertCompletedSuccessfully);
    }

    @RepeatedTest(100)
    public void yetAnotherSubscriberRace() {
        ExecutorService pool = Executors.newFixedThreadPool(32);
        Uni<Integer> uni = Uni.createFrom().deferred(() -> Uni.createFrom().item(0)).cache().emitOn(pool)
                .runSubscriptionOn(pool);

        ConcurrentLinkedDeque<UniAssertSubscriber<Integer>> subscribers = new ConcurrentLinkedDeque<>();
        AtomicInteger counter = new AtomicInteger();

        Runnable work = () -> {
            UniAssertSubscriber<Integer> subscriber = new UniAssertSubscriber<>(false);
            subscribers.add(subscriber);
            uni.onItem().invoke(counter::incrementAndGet).subscribe().withSubscriber(subscriber);
        };
        for (int i = 0; i < 10_000; i++) {
            pool.execute(work);
            counter.incrementAndGet();
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS).and().untilAtomic(counter, CoreMatchers.is(20_000));
        assertThat(subscribers).hasSize(10_000);
        subscribers.forEach(UniAssertSubscriber::assertCompletedSuccessfully);
    }

    @Test
    public void guardInvalidation() {
        AtomicBoolean condition = new AtomicBoolean(true);
        AtomicInteger subscriptionCount = new AtomicInteger();

        Uni<Integer> uni = Uni.createFrom().item(69)
                .onSubscribe().invoke(subscriptionCount::incrementAndGet)
                .cacheWhilst(condition::get);

        UniAssertSubscriber<Integer> subscriber = UniAssertSubscriber.create();
        uni.subscribe().withSubscriber(subscriber);
        subscriber.assertCompletedSuccessfully().assertItem(69);
        assertThat(subscriptionCount.get()).isEqualTo(1);

        subscriber = UniAssertSubscriber.create();
        uni.subscribe().withSubscriber(subscriber);
        subscriber.assertCompletedSuccessfully().assertItem(69);
        assertThat(subscriptionCount.get()).isEqualTo(1);

        condition.set(false);
        subscriber = UniAssertSubscriber.create();
        uni.subscribe().withSubscriber(subscriber);
        subscriber.assertCompletedSuccessfully().assertItem(69);
        assertThat(subscriptionCount.get()).isEqualTo(2);

        condition.set(true);
        subscriber = UniAssertSubscriber.create();
        uni.subscribe().withSubscriber(subscriber);
        subscriber.assertCompletedSuccessfully().assertItem(69);
        assertThat(subscriptionCount.get()).isEqualTo(2);
    }

    @RepeatedTest(100)
    public void yetAnotherSubscriberRaceWithGuardInvalidations() {
        ExecutorService pool = Executors.newFixedThreadPool(32);
        AtomicBoolean guard = new AtomicBoolean(true);

        ConcurrentLinkedDeque<UniAssertSubscriber<Integer>> subscribers = new ConcurrentLinkedDeque<>();
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger invalidationCount = new AtomicInteger();

        Uni<Integer> uni = Uni.createFrom().deferred(() -> Uni.createFrom().item(0))
                .onSubscribe().invoke(invalidationCount::incrementAndGet)
                .cacheWhilst(guard::get)
                .emitOn(pool)
                .runSubscriptionOn(pool);

        Runnable work = () -> {
            UniAssertSubscriber<Integer> subscriber = new UniAssertSubscriber<>(false);
            subscribers.add(subscriber);
            uni.onItem().invoke(counter::incrementAndGet).subscribe().withSubscriber(subscriber);
            guard.set(ThreadLocalRandom.current().nextBoolean());
        };
        for (int i = 0; i < 10_000; i++) {
            pool.execute(work);
            counter.incrementAndGet();
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS).and().untilAtomic(counter, CoreMatchers.is(20_000));
        assertThat(subscribers).hasSize(10_000);
        subscribers.forEach(UniAssertSubscriber::assertCompletedSuccessfully);
        assertThat(invalidationCount.get()).isBetween(2, 9999);
    }
}
