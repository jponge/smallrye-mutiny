package io.smallrye.mutiny.operators.multi;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.jetbrains.annotations.NotNull;

public class MultiOnCancellationCall<T> extends AbstractMultiOperator<T, T> {

    @NotNull
    private final Supplier<Uni<?>> supplier;

    public MultiOnCancellationCall(@NotNull Multi<? extends T> upstream, @NotNull Supplier<Uni<?>> supplier) {
        super(nonNull(upstream, "upstream"));
        this.supplier = nonNull(supplier, "supplier");
    }

    @Override
    public void subscribe(@NotNull MultiSubscriber<? super T> downstream) {
        upstream.subscribe().withSubscriber(new MultiOnCancellationCallProcessor(downstream));
    }

    class MultiOnCancellationCallProcessor extends MultiOperatorProcessor<T, T> {

        private final AtomicBoolean supplierInvoked = new AtomicBoolean();

        public MultiOnCancellationCallProcessor(@NotNull MultiSubscriber<? super T> downstream) {
            super(downstream);
        }

        @Override
        public void onCompletion() {
            supplierInvoked.set(true);
            super.onCompletion();
        }

        @Override
        public void cancel() {
            execute().subscribe().with(
                    context(),
                    ignoredItem -> super.cancel(),
                    ignoredFailure -> super.cancel());
        }

        private Uni<?> execute() {
            if (supplierInvoked.compareAndSet(false, true)) {
                try {
                    return nonNull(supplier.get(), "uni");
                } catch (Throwable err) {
                    return Uni.createFrom().failure(err);
                }
            } else {
                return Uni.createFrom().nullItem();
            }
        }
    }
}
