package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.LongFunction;
import java.util.function.Predicate;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.MultiBroadcast;
import io.smallrye.mutiny.groups.MultiCollect;
import io.smallrye.mutiny.groups.MultiConvert;
import io.smallrye.mutiny.groups.MultiDemandPacing;
import io.smallrye.mutiny.groups.MultiDemandPausing;
import io.smallrye.mutiny.groups.MultiGroup;
import io.smallrye.mutiny.groups.MultiIfNoItem;
import io.smallrye.mutiny.groups.MultiOnCancel;
import io.smallrye.mutiny.groups.MultiOnCompletion;
import io.smallrye.mutiny.groups.MultiOnFailure;
import io.smallrye.mutiny.groups.MultiOnItem;
import io.smallrye.mutiny.groups.MultiOnRequest;
import io.smallrye.mutiny.groups.MultiOnSubscribe;
import io.smallrye.mutiny.groups.MultiOnTerminate;
import io.smallrye.mutiny.groups.MultiOverflow;
import io.smallrye.mutiny.groups.MultiSelect;
import io.smallrye.mutiny.groups.MultiSkip;
import io.smallrye.mutiny.groups.MultiSubscribe;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.MultiCacheOp;
import io.smallrye.mutiny.operators.multi.MultiDemandCapping;
import io.smallrye.mutiny.operators.multi.MultiEmitOnOp;
import io.smallrye.mutiny.operators.multi.MultiLogger;
import io.smallrye.mutiny.operators.multi.MultiSubscribeOnOp;
import io.smallrye.mutiny.operators.multi.MultiWithContext;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.subscription.MultiSubscriberAdapter;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractMulti<T> implements Multi<T> {

    public void subscribe(MultiSubscriber<? super T> subscriber) {
        this.subscribe(Infrastructure.onMultiSubscription(this, subscriber));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        // NOTE The Reactive Streams TCK mandates throwing an NPE.
        Objects.requireNonNull(subscriber, "Subscriber is `null`");
        MultiSubscriber<? super T> actual;
        if (subscriber instanceof MultiSubscriber) {
            actual = (MultiSubscriber<? super T>) subscriber;
        } else {
            actual = new MultiSubscriberAdapter<>(subscriber);
        }
        this.subscribe(actual);
    }

    @NotNull
    @Override
    public MultiOnItem<T> onItem() {
        return new MultiOnItem<>(this);
    }

    @NotNull
    @Override
    public MultiSubscribe<T> subscribe() {
        return new MultiSubscribe<>(this);
    }

    @Override
    public Uni<T> toUni() {
        return Uni.createFrom().publisher(this);
    }

    @NotNull
    @Override
    public MultiOnFailure<T> onFailure() {
        return new MultiOnFailure<>(this, null);
    }

    @NotNull
    @Override
    public MultiOnFailure<T> onFailure(Predicate<? super Throwable> predicate) {
        return new MultiOnFailure<>(this, predicate);
    }

    @NotNull
    @Override
    public MultiOnFailure<T> onFailure(@NotNull Class<? extends Throwable> typeOfFailure) {
        return new MultiOnFailure<>(this, typeOfFailure::isInstance);
    }

    @NotNull
    @Override
    public MultiIfNoItem<T> ifNoItem() {
        return new MultiIfNoItem<>(this);
    }

    @Override
    public Multi<T> cache() {
        return Infrastructure.onMultiCreation(new MultiCacheOp<>(this));
    }

    @Override
    public Multi<T> emitOn(@NotNull Executor executor) {
        return emitOn(executor, Infrastructure.getBufferSizeS());
    }

    @Override
    public Multi<T> emitOn(@NotNull Executor executor, int bufferSize) {
        return Infrastructure.onMultiCreation(
                new MultiEmitOnOp<>(
                        this,
                        nonNull(executor, "executor"),
                        ParameterValidation.positive(bufferSize, "bufferSize")));
    }

    @Override
    public Multi<T> runSubscriptionOn(@NotNull Executor executor) {
        return Infrastructure.onMultiCreation(new MultiSubscribeOnOp<>(this, executor));
    }

    @NotNull
    @Override
    public MultiOnCompletion<T> onCompletion() {
        return new MultiOnCompletion<>(this);
    }

    @NotNull
    @Override
    public MultiSelect<T> select() {
        return new MultiSelect<>(this);
    }

    @NotNull
    @Override
    public MultiSkip<T> skip() {
        return new MultiSkip<>(this);
    }

    @NotNull
    @Override
    public MultiOverflow<T> onOverflow() {
        return new MultiOverflow<>(this);
    }

    @NotNull
    @Override
    public MultiOnSubscribe<T> onSubscription() {
        return new MultiOnSubscribe<>(this);
    }

    @NotNull
    @Override
    public MultiBroadcast<T> broadcast() {
        return new MultiBroadcast<>(this);
    }

    @NotNull
    @Override
    public MultiConvert<T> convert() {
        return new MultiConvert<>(this);
    }

    @NotNull
    @Override
    public MultiOnTerminate<T> onTermination() {
        return new MultiOnTerminate<>(this);
    }

    @NotNull
    @Override
    public MultiOnCancel<T> onCancellation() {
        return new MultiOnCancel<>(this);
    }

    @NotNull
    @Override
    public MultiOnRequest<T> onRequest() {
        return new MultiOnRequest<>(this);
    }

    @NotNull
    @Override
    public MultiCollect<T> collect() {
        return new MultiCollect<>(this);
    }

    @NotNull
    @Override
    public MultiGroup<T> group() {
        return new MultiGroup<>(this);
    }

    @NotNull
    public Multi<T> toHotStream() {
        BroadcastProcessor<T> processor = BroadcastProcessor.create();
        this.subscribe(processor);
        return processor;
    }

    @Override
    public Multi<T> log(@NotNull String identifier) {
        return Infrastructure.onMultiCreation(new MultiLogger<>(this, identifier));
    }

    @Override
    public Multi<T> log() {
        return log("Multi." + this.getClass().getSimpleName());
    }

    @Override
    public <R> Multi<R> withContext(@NotNull BiFunction<Multi<T>, Context, Multi<R>> builder) {
        return Infrastructure.onMultiCreation(new MultiWithContext<>(this, nonNull(builder, "builder")));
    }

    @NotNull
    @Override
    public MultiDemandPacing<T> paceDemand() {
        return new MultiDemandPacing<>(this);
    }

    @NotNull
    @Override
    public MultiDemandPausing<T> pauseDemand() {
        return new MultiDemandPausing<>(this);
    }

    @Override
    public Multi<T> capDemandsUsing(@NotNull LongFunction<Long> function) {
        return Infrastructure.onMultiCreation(new MultiDemandCapping<>(this, nonNull(function, "function")));
    }
}
