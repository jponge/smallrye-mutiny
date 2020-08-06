package io.smallrye.mutiny.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import io.smallrye.mutiny.subscription.UniEmitter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;

public class UniOnFailureTransformTest {

    private Uni<Integer> failure;

    @BeforeMethod
    public void init() {
        failure = Uni.createFrom().failure(new IOException("boom"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testThatMapperMustNotBeNull() {
        Uni.createFrom().item(1).onFailure().transform(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testThatSourceMustNotBeNull() {
        new UniOnFailureTransform<>(null, t -> true, Function.identity());
    }

    @Test
    public void testSimpleMapping() {
        UniAssertSubscriber<Integer> subscriber = failure
                .onFailure().transform(t -> new BoomException())
                .subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.assertCompletedWithFailure()
                .assertFailure(BoomException.class, "BoomException");
    }

    @Test
    public void testWithTwoSubscribers() {
        UniAssertSubscriber<Integer> ts1 = UniAssertSubscriber.create();
        UniAssertSubscriber<Integer> ts2 = UniAssertSubscriber.create();

        AtomicInteger count = new AtomicInteger();
        Uni<Integer> uni = failure.onFailure().transform(t -> new BoomException(count.incrementAndGet()));
        uni.subscribe().withSubscriber(ts1);
        uni.subscribe().withSubscriber(ts2);

        ts1.assertCompletedWithFailure()
                .assertFailure(BoomException.class, "1");
        ts2.assertCompletedWithFailure()
                .assertFailure(BoomException.class, "2");
    }

    @Test
    public void testWhenTheMapperThrowsAnException() {
        UniAssertSubscriber<Object> ts = UniAssertSubscriber.create();

        failure.onFailure().transform(t -> {
            throw new RuntimeException("failure");
        }).subscribe().withSubscriber(ts);

        ts.assertFailure(RuntimeException.class, "failure");
    }

    @Test
    public void testThatMapperCanNotReturnNull() {
        UniAssertSubscriber<Object> ts = UniAssertSubscriber.create();

        failure.onFailure().transform(t -> null).subscribe().withSubscriber(ts);

        ts.assertFailure(NullPointerException.class, "null");
    }

    @Test
    public void testThatMapperIsCalledOnTheRightExecutor() {
        UniAssertSubscriber<Integer> ts = new UniAssertSubscriber<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<String> threadName = new AtomicReference<>();
            failure
                    .emitOn(executor)
                    .onFailure().transform(fail -> {
                        threadName.set(Thread.currentThread().getName());
                        return new BoomException();
                    })
                    .subscribe().withSubscriber(ts);

            ts.await().assertFailure(BoomException.class, "BoomException");
            assertThat(threadName).isNotNull().doesNotHaveValue("main");
            assertThat(ts.getOnFailureThreadName()).isEqualTo(threadName.get());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testThatMapperIsNotCalledOnItem() {
        UniAssertSubscriber<Integer> ts = UniAssertSubscriber.create();
        AtomicBoolean called = new AtomicBoolean();
        Uni.createFrom().item(1)
                .onFailure().transform(f -> {
                    called.set(true);
                    return f;
                })
                .subscribe().withSubscriber(ts);
        ts.assertItem(1);
        assertThat(called).isFalse();
    }

    @Test
    public void testThatMapperIsNotCalledOnNonMatchingPredicate() {
        UniAssertSubscriber<Integer> ts = UniAssertSubscriber.create();
        AtomicBoolean called = new AtomicBoolean();
        Uni.createFrom().<Integer> failure(new IllegalStateException("boom"))
                .onFailure(IOException.class).transform(f -> {
                    called.set(true);
                    return new IllegalArgumentException("Karamba");
                })
                .subscribe().withSubscriber(ts);
        ts.assertCompletedWithFailure().assertFailure(IllegalStateException.class, "boom");
        assertThat(called).isFalse();
    }

    @Test
    public void testThatMapperIsNotCalledWhenPredicateThrowsAnException() {
        UniAssertSubscriber<Integer> ts = UniAssertSubscriber.create();
        AtomicBoolean called = new AtomicBoolean();
        Uni.createFrom().<Integer> failure(new IllegalStateException("boom"))
                .onFailure(t -> {
                    throw new IllegalArgumentException("boomboom");
                }).transform(f -> {
                    called.set(true);
                    return new RuntimeException("Karamba");
                })
                .subscribe().withSubscriber(ts);
        ts.assertCompletedWithFailure()
                .assertFailure(CompositeException.class, "boomboom")
                .assertFailure(CompositeException.class, " boom");
        assertThat(called).isFalse();
    }

    @Test
    public void verifyThatTheMapperIsNotCalledAfterCancellationWithEmitter() {
        AtomicReference<UniEmitter<? super Integer>> emitter = new AtomicReference<>();
        AtomicBoolean called = new AtomicBoolean();
        UniAssertSubscriber<Integer> subscriber = Uni.createFrom()
                .emitter((Consumer<UniEmitter<? super Integer>>) emitter::set)
                .onFailure().transform(failure -> {
                    called.set(true);
                    return new ArithmeticException(failure.getMessage());
                })
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        assertThat(called).isFalse();
        subscriber.assertNotCompleted().assertNoFailure().assertSubscribed();
        subscriber.cancel();
        emitter.get().fail(new IOException("boom"));
        assertThat(called).isFalse();
    }

    @Test
    public void verifyThatTheMapperIsNotCalledAfterImmediateCancellationWithEmitter() {
        AtomicReference<UniEmitter<? super Integer>> emitter = new AtomicReference<>();
        AtomicBoolean called = new AtomicBoolean();
        UniAssertSubscriber<Integer> subscriber = Uni.createFrom()
                .emitter((Consumer<UniEmitter<? super Integer>>) emitter::set)
                .onFailure().transform(failure -> {
                    called.set(true);
                    return new ArithmeticException(failure.getMessage());
                })
                .subscribe().withSubscriber(new UniAssertSubscriber<>(true));

        assertThat(called).isFalse();
        subscriber.assertNotCompleted().assertNoFailure().assertSubscribed();
        subscriber.cancel();
        emitter.get().fail(new IOException("boom"));
        assertThat(called).isFalse();
    }

    private static class BoomException extends Exception {
        BoomException() {
            super("BoomException");
        }

        BoomException(int count) {
            super(Integer.toString(count));
        }
    }

}
