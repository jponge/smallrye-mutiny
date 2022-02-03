package io.smallrye.mutiny.groups;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.builders.MultiReplayPublisher;

import java.time.Duration;
import java.util.List;

import static io.smallrye.mutiny.helpers.ParameterValidation.*;
import static java.util.Arrays.asList;

public class MultiReplay {

    static private class Builder<T> implements WithItemsAndCancellationPolicy<T>, AtMost<T> {

        private int count;
        private Multi<T> multi;
        private boolean cancelAfterLastSubscriberLeaves;
        private Duration cancellationDelay;

        public Builder(Multi<T> multi) {
            this.multi = nonNull(multi, "multi");
        }

        @Override
        public WithItemsAndCancellationPolicy<T> cancelAfterLastSubscriberLeaves() {
            this.cancelAfterLastSubscriberLeaves = true;
            return this;
        }

        @Override
        public WithItemsAndCancellationPolicy<T> cancelAfterLastSubscriberLeaves(Duration delay) {
            this.cancelAfterLastSubscriberLeaves = true;
            this.cancellationDelay = nonNull(delay, "delay");
            return this;
        }

        @Override
        public Multi<T> withInitialValues(T... values) {
            List<T> initialValues = asList(doesNotContainNull(values, "values"));
            return withInitialValues(initialValues);
        }

        @Override
        public Multi<T> withInitialValues(Iterable<T> iterable) {
            Iterable<T> initialValues = doesNotContainNull(iterable, "iterable");
            return new MultiReplayPublisher<>(count, multi, initialValues, cancelAfterLastSubscriberLeaves, cancellationDelay);
        }

        @Override
        public WithItemsAndCancellationPolicy<T> atMost(int count) {
            this.count = positive(count, "count");
            return this;
        }
    }

    public interface WithItemsAndCancellationPolicy<T> {
        @CheckReturnValue
        WithItemsAndCancellationPolicy<T> cancelAfterLastSubscriberLeaves();

        @CheckReturnValue
        WithItemsAndCancellationPolicy<T> cancelAfterLastSubscriberLeaves(Duration delay);

        @CheckReturnValue
        Multi<T> withInitialValues(T... values);

        @CheckReturnValue
        Multi<T> withInitialValues(Iterable<T> iterable);
    }

    public interface AtMost<T> {
        @CheckReturnValue
        WithItemsAndCancellationPolicy<T> atMost(int count);
    }

    @CheckReturnValue
    public <T> AtMost<T> from(Multi<T> multi) {
        return new Builder<>(multi);
    }
}
