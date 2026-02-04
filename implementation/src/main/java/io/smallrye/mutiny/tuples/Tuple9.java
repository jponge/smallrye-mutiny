package io.smallrye.mutiny.tuples;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9> extends Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> implements Tuple { // NOSONAR

    final T9 item9;

    Tuple9(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g, T8 h, T9 i) { // NOSONAR
        super(a, b, c, d, e, f, g, h);
        this.item9 = i;
    }

    @NotNull
    public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T9> of(T1 a, T2 b, T3 c, T4 d, //NOSONAR
                                                                                                     T5 e, T6 f, T7 g, T8 h, T9 i) {
        return new Tuple9<>(a, b, c, d, e, f, g, h, i);
    }

    public T9 getItem9() {
        return item9;
    }

    @NotNull
    @Override
    public <T> Tuple9<T, T2, T3, T4, T5, T6, T7, T8, T9> mapItem1(@NotNull Function<T1, T> mapper) {
        return Tuple9.of(mapper.apply(item1), item2, item3, item4, item5, item6, item7, item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T, T3, T4, T5, T6, T7, T8, T9> mapItem2(@NotNull Function<T2, T> mapper) {
        return Tuple9.of(item1, mapper.apply(item2), item3, item4, item5, item6, item7, item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T2, T, T4, T5, T6, T7, T8, T9> mapItem3(@NotNull Function<T3, T> mapper) {
        return Tuple9.of(item1, item2, mapper.apply(item3), item4, item5, item6, item7, item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T2, T3, T, T5, T6, T7, T8, T9> mapItem4(@NotNull Function<T4, T> mapper) {
        return Tuple9.of(item1, item2, item3, mapper.apply(item4), item5, item6, item7, item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T2, T3, T4, T, T6, T7, T8, T9> mapItem5(@NotNull Function<T5, T> mapper) {
        return Tuple9.of(item1, item2, item3, item4, mapper.apply(item5), item6, item7, item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T2, T3, T4, T5, T, T7, T8, T9> mapItem6(@NotNull Function<T6, T> mapper) {
        return Tuple9.of(item1, item2, item3, item4, item5, mapper.apply(item6), item7, item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T2, T3, T4, T5, T6, T, T8, T9> mapItem7(@NotNull Function<T7, T> mapper) {
        return Tuple9.of(item1, item2, item3, item4, item5, item6, mapper.apply(item7), item8, item9);
    }

    @NotNull
    @Override
    public <T> Tuple9<T1, T2, T3, T4, T5, T6, T7, T, T9> mapItem8(@NotNull Function<T8, T> mapper) {
        return Tuple9.of(item1, item2, item3, item4, item5, item6, item7, mapper.apply(item8), item9);
    }

    @NotNull
    public <T> Tuple9<T1, T2, T3, T4, T5, T6, T7, T8, T> mapItem9(@NotNull Function<T9, T> mapper) {
        return Tuple9.of(item1, item2, item3, item4, item5, item6, item7, item8, mapper.apply(item9));
    }

    @Override
    public Object nth(int index) {
        assertIndexInBounds(index);

        if (index == 8) {
            return item9;
        } else {
            return super.nth(index);
        }
    }

    @NotNull
    @Override
    public List<Object> asList() {
        return Arrays.asList(item1, item2, item3, item4, item5, item6, item7, item8, item9);
    }

    @Override
    public int size() {
        return 9;
    }

    @NotNull
    @Override
    public String toString() {
        return "Tuple{" +
                "item1=" + item1 +
                ",item2=" + item2 +
                ",item3=" + item3 +
                ",item4=" + item4 +
                ",item5=" + item5 +
                ",item6=" + item6 +
                ",item7=" + item7 +
                ",item8=" + item8 +
                ",item9=" + item9 +
                '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Tuple9<?, ?, ?, ?, ?, ?, ?, ?, ?> tuple = (Tuple9<?, ?, ?, ?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(item9, tuple.item9);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), item9);
    }
}
