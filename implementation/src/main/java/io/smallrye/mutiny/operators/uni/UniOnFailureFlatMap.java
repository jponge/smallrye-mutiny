package io.smallrye.mutiny.operators.uni;

import static io.smallrye.mutiny.helpers.ParameterValidation.MAPPER_RETURNED_NULL;
import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.function.Function;
import java.util.function.Predicate;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.AbstractUni;
import io.smallrye.mutiny.operators.UniOperator;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.subscription.UniSubscription;
import org.jetbrains.annotations.NotNull;

public class UniOnFailureFlatMap<I, E> extends UniOperator<I, I> {

    @NotNull
    private final Function<E, Uni<? extends I>> mapper;
    @NotNull
    private final Predicate<? super Throwable> predicate;
    @NotNull
    private final Class<E> typeOfFailure;

    public UniOnFailureFlatMap(@NotNull Uni<I> upstream,
                               @NotNull Predicate<? super Throwable> predicate,
                               @NotNull Function<E, Uni<? extends I>> mapper,
                               @NotNull Class<E> typeOfFailure) {
        super(nonNull(upstream, "upstream"));
        this.mapper = nonNull(mapper, "mapper");
        this.predicate = nonNull(predicate, "predicate");
        this.typeOfFailure = nonNull(typeOfFailure, "typeOfFailure");
    }

    public void subscribe(UniSubscriber<? super I> subscriber) {
        AbstractUni.subscribe(upstream(), new UniOnFailureFlatMapProcessor(subscriber));
    }

    private class UniOnFailureFlatMapProcessor extends UniOperatorProcessor<I, I> {

        private volatile UniSubscription innerSubscription;

        public UniOnFailureFlatMapProcessor(UniSubscriber<? super I> downstream) {
            super(downstream);
        }

        @Override
        public void onSubscribe(@NotNull UniSubscription subscription) {
            if (getCurrentUpstreamSubscription() == null) {
                super.onSubscribe(subscription);
            } else if (innerSubscription == null) {
                this.innerSubscription = subscription;
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onFailure(Throwable failure) {
            if (isCancelled()) {
                Infrastructure.handleDroppedException(failure);
                return;
            }
            if (innerSubscription == null) {
                dispatch(failure);
            } else {
                downstream.onFailure(failure);
            }
        }

        @Override
        public void cancel() {
            if (innerSubscription != null) {
                innerSubscription.cancel();
            }
            super.cancel();
        }

        private void dispatch(Throwable failure) {
            boolean test;
            try {
                test = predicate.test(failure);
            } catch (Throwable err) {
                downstream.onFailure(new CompositeException(failure, err));
                return;
            }
            if (test) {
                performInnerSubscription(failure);
            } else {
                downstream.onFailure(failure);
            }
        }

        private void performInnerSubscription(Throwable failure) {
            Uni<? extends I> uni;
            try {
                uni = mapper.apply(typeOfFailure.cast(failure));
            } catch (Throwable err) {
                downstream.onFailure(new CompositeException(failure, err));
                return;
            }
            if (uni == null) {
                downstream.onFailure(new NullPointerException(MAPPER_RETURNED_NULL));
                return;
            }
            AbstractUni.subscribe(uni, this);
        }
    }
}
