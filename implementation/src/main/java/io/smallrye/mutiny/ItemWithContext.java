package io.smallrye.mutiny;

import java.util.Objects;

// TODO
public final class ItemWithContext<T> {

    private final Context context;
    private final T item;

    public ItemWithContext(Context context, T item) {
        this.context = context;
        this.item = item;
    }

    public Context context() {
        return context;
    }

    public T get() {
        return item;
    }

    @Override
    public String toString() {
        return "ItemWithContext{" +
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
        ItemWithContext<?> that = (ItemWithContext<?>) o;
        return Objects.equals(context, that.context) && Objects.equals(item, that.item);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, item);
    }
}
