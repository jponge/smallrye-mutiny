package io.smallrye.mutiny.operators.multi;

import static io.smallrye.mutiny.helpers.MultiSubscribers.toMultiSubscriber;
import static org.mockito.Mockito.*;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import org.junit.jupiter.api.Test;

import io.reactivex.rxjava3.processors.PublishProcessor;
import io.smallrye.mutiny.Multi;
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
}
