package io.smallrye.mutiny.operators;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.converters.uni.UniToMultiPublisher;
import io.smallrye.mutiny.groups.*;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.uni.*;
import io.smallrye.mutiny.subscription.UniSubscriber;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractUni<T> implements Uni<T> {

    public abstract void subscribe(UniSubscriber<? super T> subscriber);

    /**
     * Encapsulates subscription to slightly optimize the AbstractUni case.
     * <p>
     * In the case of AbstractUni, it avoid creating the UniSubscribe group instance.
     *
     * @param upstream the upstream, must not be {@code null} (not checked)
     * @param subscriber the subscriber, must not be {@code null} (not checked)
     * @param <T> the type of item
     */
    @SuppressWarnings("unchecked")
    public static <T> void subscribe(Uni<? extends T> upstream, @NotNull UniSubscriber<? super T> subscriber) {
        if (upstream instanceof AbstractUni abstractUni) {
            UniSubscriber actualSubscriber = Infrastructure.onUniSubscription(upstream, subscriber);
            abstractUni.subscribe(actualSubscriber);
        } else {
            upstream.subscribe().withSubscriber(subscriber);
        }
    }

    @NotNull
    @Override
    public UniSubscribe<T> subscribe() {
        return new UniSubscribe<>(this);
    }

    @NotNull
    @Override
    public UniOnItem<T> onItem() {
        return new UniOnItem<>(this);
    }

    @NotNull
    @Override
    public UniIfNoItem<T> ifNoItem() {
        return new UniIfNoItem<>(this);
    }

    @NotNull
    @Override
    public UniOnFailure<T, Throwable> onFailure() {
        return new UniOnFailure<>(this, Throwable.class, null);
    }

    @NotNull
    @Override
    public UniOnFailure<T, Throwable> onFailure(Predicate<? super Throwable> predicate) {
        return new UniOnFailure<>(this, Throwable.class, predicate);
    }

    @NotNull
    @Override
    public <E extends Throwable> UniOnFailure<T, E> onFailure(@NotNull Class<E> typeOfFailure) {
        return new UniOnFailure<>(this, typeOfFailure, typeOfFailure::isInstance);
    }

    @NotNull
    @Override
    public UniOnSubscribe<T> onSubscription() {
        return new UniOnSubscribe<>(this);
    }

    @NotNull
    @Override
    public UniOnItemOrFailure<T> onItemOrFailure() {
        return new UniOnItemOrFailure<>(this);
    }

    @Override
    public UniAwait<T> await() {
        return awaitUsing(Context.empty());
    }

    @NotNull
    @Override
    public UniAwait<T> awaitUsing(Context context) {
        return new UniAwait<>(this, context);
    }

    @Override
    public Uni<T> emitOn(@NotNull Executor executor) {
        return Infrastructure.onUniCreation(
                new UniEmitOn<>(this, nonNull(executor, "executor")));
    }

    @Override
    public Uni<T> runSubscriptionOn(@NotNull Executor executor) {
        return Infrastructure.onUniCreation(
                new UniRunSubscribeOn<>(this, executor));
    }

    @NotNull
    @Override
    public UniMemoize<T> memoize() {
        return new UniMemoize<>(this);
    }

    public Uni<T> cache() {
        return Infrastructure.onUniCreation(new UniMemoizeOp<>(this));
    }

    @NotNull
    @Override
    public UniConvert<T> convert() {
        return new UniConvert<>(this);
    }

    @Override
    public Multi<T> toMulti() {
        return Multi.createFrom().safePublisher(new UniToMultiPublisher<>(this));
    }

    @NotNull
    @Override
    public UniRepeat<T> repeat() {
        return new UniRepeat<>(this);
    }

    @NotNull
    @Override
    public UniOnTerminate<T> onTermination() {
        return new UniOnTerminate<>(this);
    }

    @NotNull
    @Override
    public UniOnCancel<T> onCancellation() {
        return new UniOnCancel<>(this);
    }

    @Override
    public Uni<T> log(@NotNull String identifier) {
        return Infrastructure.onUniCreation(new UniLogger<>(this, identifier));
    }

    @Override
    public Uni<T> log() {
        return log("Uni." + this.getClass().getSimpleName());
    }

    @Override
    public <R> Uni<R> withContext(@NotNull BiFunction<Uni<T>, Context, Uni<R>> builder) {
        return Infrastructure.onUniCreation(new UniWithContext<>(this, nonNull(builder, "builder")));
    }
}
