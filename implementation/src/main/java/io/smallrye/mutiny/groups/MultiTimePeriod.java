package io.smallrye.mutiny.groups;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.validate;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.builders.IntervalMulti;
import org.jetbrains.annotations.NotNull;

public class MultiTimePeriod {

    private Duration initialDelay;
    private ScheduledExecutorService executor;

    @NotNull
    @CheckReturnValue
    public MultiTimePeriod startingAfter(Duration duration) {
        this.initialDelay = validate(duration, "duration");
        return this;
    }

    @NotNull
    @CheckReturnValue
    public MultiTimePeriod onExecutor(ScheduledExecutorService executor) {
        this.executor = nonNull(executor, "executor");
        return this;
    }

    @CheckReturnValue
    public Multi<Long> every(Duration duration) {
        validate(duration, "duration");
        ScheduledExecutorService executorService = this.executor;
        if (executorService == null) {
            executorService = Infrastructure.getDefaultWorkerPool();
        }
        if (initialDelay != null) {
            return Infrastructure.onMultiCreation(new IntervalMulti(initialDelay, duration, executorService));
        } else {
            return Infrastructure.onMultiCreation(new IntervalMulti(duration, executorService));
        }
    }

}
