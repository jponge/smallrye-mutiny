package io.smallrye.mutiny.operators.multi;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.jetbrains.annotations.NotNull;

public class MultiOnRequestCall<T> extends AbstractMultiOperator<T, T> {

    @NotNull
    private final LongFunction<Uni<?>> mapper;

    public MultiOnRequestCall(@NotNull Multi<? extends T> upstream, @NotNull LongFunction<Uni<?>> mapper) {
        super(nonNull(upstream, "upstream"));
        this.mapper = nonNull(mapper, "consumer");
    }

    @Override
    public void subscribe(@NotNull MultiSubscriber<? super T> downstream) {
        upstream.subscribe().withSubscriber(new MultiOnRequestCallOperator(nonNull(downstream, "downstream")));
    }

    class MultiOnRequestCallOperator extends MultiOperatorProcessor<T, T> {

        public MultiOnRequestCallOperator(MultiSubscriber<? super T> downstream) {
            super(downstream);
        }

        // Note that as per reactive streams specifications request and cancel calls need to happen serially
        private final AtomicReference<Cancellable> cancellable = new AtomicReference<>();

        @Override
        public void request(long numberOfItems) {
            if (numberOfItems <= 0) {
                onFailure(new IllegalArgumentException("Invalid number of request, must be greater than 0"));
                return;
            }
            cancellable.set(execute(numberOfItems).subscribe().with(
                    context(),
                    ignored -> {
                        cancellable.set(null);
                        super.request(numberOfItems);
                    },
                    throwable -> {
                        cancellable.set(null);
                        super.onFailure(throwable);
                    }));
        }

        @Override
        public void cancel() {
            super.cancel();
            Cancellable uni = cancellable.getAndSet(null);
            if (uni != null) {
                uni.cancel();
            }
        }

        private Uni<?> execute(long numberOfItems) {
            try {
                return nonNull(mapper.apply(numberOfItems), "uni");
            } catch (Throwable err) {
                return Uni.createFrom().failure(err);
            }
        }
    }
}
