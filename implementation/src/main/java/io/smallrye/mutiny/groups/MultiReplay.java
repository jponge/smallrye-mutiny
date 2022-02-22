package io.smallrye.mutiny.groups;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.replay.ReplayOperator;

public class MultiReplay {

    private long numberOfItemsToReplay = Long.MAX_VALUE;

    public MultiReplay upTo(long numberOfItemsToReplay) {
        this.numberOfItemsToReplay = positive(numberOfItemsToReplay, "numberOfItemsToReplay");
        return this;
    }

    public <T> Multi<T> ofMulti(Multi<T> upstream) {
        return new ReplayOperator<>(nonNull(upstream, "upstream"), numberOfItemsToReplay);
    }
}
