package io.smallrye.mutiny.groups;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;

class MultiBroadcasterTest {

    @Test
    void playground_1() throws InterruptedException {
        Multi<String> multi = Multi.createFrom().ticks().every(Duration.ofMillis(500))
                .select().first(5)
                .onItem().transform(Object::toString);

        Multi<String> broadcast = Multi.createBy().broadcasting().unthrottled(multi)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor());

        CountDownLatch latch = new CountDownLatch(2);
        broadcast.subscribe().with(tick -> System.out.println("#1 " + tick), Throwable::printStackTrace, latch::countDown);
        broadcast.subscribe().with(tick -> System.out.println("#2 " + tick), Throwable::printStackTrace, latch::countDown);
        latch.await();
    }

    @Test
    void playground_2() {
        Multi<String> multi = Multi.createFrom().range(1, 6)
                .onItem().transform(Object::toString);

        AssertSubscriber<String> sub = Multi.createBy().broadcasting().unthrottled(multi)
                .subscribe().withSubscriber(AssertSubscriber.create());

        System.out.println(sub.getItems());
        sub.request(2);
        System.out.println(sub.getItems());
        sub.request(1);
        System.out.println(sub.getItems());
        sub.request(10);
        System.out.println(sub.getItems());
        sub.assertCompleted().assertItems("1", "2", "3", "4", "5");
    }

    @Test
    void playground_3() {
        Multi<Object> multi = Multi.createFrom().emitter(em -> {
            em.emit("foo");
            em.emit("bar");
            em.fail(new IOException("boom"));
        });

        Multi<Object> broadcaster = Multi.createBy().broadcasting().unthrottled(multi)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor());

        AssertSubscriber<Object> sub = broadcaster.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        sub.awaitFailure().assertItems("foo", "bar").assertFailedWith(IOException.class, "boom");

        sub = broadcaster.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        sub.awaitFailure().assertHasNotReceivedAnyItem().assertFailedWith(IOException.class, "boom");
    }

    @Test
    void playground_4() {
        AtomicBoolean step1 = new AtomicBoolean();
        AtomicBoolean step2 = new AtomicBoolean();

        Multi<Object> multi = Multi.createFrom().emitter(em -> {
            em.emit("foo");
            em.emit("bar");
            Awaitility.await().untilTrue(step1);
            em.emit("baz");
            step2.set(true);
        });

        MultiBroadcaster.UnthrottledConf conf = MultiBroadcaster.UnthrottledConf.newBuilder()
                .cancelAfterLastSubscriber(true)
                .subscriberInitialQueueSize(256)
                .build();
        Multi<Object> broadcaster = Multi.createBy().broadcasting().unthrottled(multi, conf)
                .runSubscriptionOn(Infrastructure.getDefaultExecutor());

        AssertSubscriber<Object> sub = broadcaster.subscribe().withSubscriber(AssertSubscriber.create());
        sub.request(2);
        sub.awaitItems(2);
        sub.request(20);
        System.out.println(sub.getItems());
        step1.set(true);
        Awaitility.await().untilTrue(step2);
        System.out.println(sub.getItems());
    }

    @Test
    void playground_5() throws InterruptedException {
        Multi<String> multi = Multi.createFrom().ticks().every(Duration.ofMillis(500))
                .onItem().transform(Object::toString);

        Multi<String> broadcast = Multi.createBy().broadcasting()
                .unthrottled(multi, MultiBroadcaster.UnthrottledConf.newBuilder().cancelAfterLastSubscriber(true).build())
                .runSubscriptionOn(Infrastructure.getDefaultExecutor());

        AssertSubscriber<String> sub = broadcast.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        Thread.sleep(3000);
        sub.cancel();
        System.out.println(sub.getItems());
        sub.assertNotTerminated();
        Thread.sleep(500);
        System.out.println(sub.getItems());
        sub.assertNotTerminated();

        sub = broadcast.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        Thread.sleep(1000);
        sub.awaitCompletion().assertHasNotReceivedAnyItem();
    }

    @Test
    void playground_6() throws InterruptedException {
        Multi<String> multi = Multi.createFrom().ticks().every(Duration.ofMillis(500))
                .onItem().transform(Object::toString);

        Multi<String> broadcast = Multi.createBy().broadcasting()
                .unthrottled(multi,
                        MultiBroadcaster.UnthrottledConf.newBuilder().cancelAfterLastSubscriberDelay(Duration.ofSeconds(1))
                                .build())
                .runSubscriptionOn(Infrastructure.getDefaultExecutor());

        AssertSubscriber<String> sideSub = broadcast.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        AssertSubscriber<String> sub = broadcast.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        Thread.sleep(1000);
        sub.cancel();
        System.out.println(sub.getItems());

        Thread.sleep(500);
        sub = broadcast.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        Thread.sleep(3000);
        sub.cancel();
        System.out.println(sub.getItems());

        sideSub.cancel();
        System.out.println("> " + sideSub.getItems());

        Thread.sleep(1500);
        System.out.println("Zzz for too long");

        sub = broadcast.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        Thread.sleep(1000);
        sub.awaitCompletion().assertHasNotReceivedAnyItem();
    }
}
