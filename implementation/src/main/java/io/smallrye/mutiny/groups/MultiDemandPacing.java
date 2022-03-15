package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.operators.multi.MultiDemandPacer;
import io.smallrye.mutiny.subscription.DemandPacer;

import java.util.concurrent.ScheduledExecutorService;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

public class MultiDemandPacing<T> {

    private final AbstractMulti<T> upstream;
    private ScheduledExecutorService executor = Infrastructure.getDefaultWorkerPool();

    public MultiDemandPacing(AbstractMulti<T> upstream) {
        this.upstream = upstream;
    }

    public MultiDemandPacing<T> on(ScheduledExecutorService executor) {
        this.executor = nonNull(executor, "executor");
        return this;
    }

    public Multi<T> using(DemandPacer pacer) {
        return Infrastructure.onMultiCreation(new MultiDemandPacer<>(upstream, executor, nonNull(pacer, "pacer")));
    }
}
