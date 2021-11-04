package io.smallrye.mutiny;

import java.util.Objects;

// TODO
public final class ContextAndItem<T> {

    private final Context context;
    private final T item;

    public ContextAndItem(Context context, T item) {
        this.context = context;
        this.item = item;
    }

    public Context context() {
        return context;
    }

    public T item() {
        return item;
    }

    @Override
    public String toString() {
        return "ContextAndItem{" +
                "context=" + context +
                ", item=" + item +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContextAndItem<?> that = (ContextAndItem<?>) o;
        return Objects.equals(context, that.context) && Objects.equals(item, that.item);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, item);
    }
}
