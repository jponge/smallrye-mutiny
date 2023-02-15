package io.smallrye.mutiny.groups;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Uni;

public class UniAwaitStream<T> {

    private final Uni<T> upstream;
    private final Context context;

    public UniAwaitStream(Uni<T> upstream, Context context) {
        this.upstream = upstream;
        this.context = context;
    }

    public <E> Stream<E> indefinitely() {
        T result = upstream.awaitUsing(context).indefinitely();
        return handleResult(result);
    }

    public <E> Stream<E> atMost(Duration duration) {
        T result = upstream.awaitUsing(context).atMost(duration);
        return handleResult(result);
    }

    @SuppressWarnings("unchecked")
    private <E> Stream<E> handleResult(T result) {
        if (result == null) {
            return Stream.empty();
        }
        if (result instanceof Collection) {
            return (Stream<E>) ((Collection<?>) result).stream();
        }
        if (result.getClass().isArray()) {
            Class<?> componentType = result.getClass().getComponentType();
            if (componentType == int.class) {
                return (Stream<E>) Arrays.stream((int[]) result).boxed();
            }
            if (componentType == double.class) {
                return (Stream<E>) Arrays.stream((double[]) result).boxed();
            }
            if (componentType == long.class) {
                return (Stream<E>) Arrays.stream((long[]) result).boxed();
            }
            return (Stream<E>) Arrays.stream((Object[]) result);
        }
        return (Stream<E>) Stream.of(result);
    }
}
