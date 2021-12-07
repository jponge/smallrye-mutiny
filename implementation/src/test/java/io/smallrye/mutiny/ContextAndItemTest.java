package io.smallrye.mutiny;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ContextAndItemTest {

    @Test
    void sanityCheck() {
        ContextAndItem<Integer> contextAndItem = new ContextAndItem<>(Context.of("foo", "bar"), 123);

        assertThat(contextAndItem.context().<String> get("foo")).isEqualTo("bar");
        assertThat(contextAndItem.item()).isEqualTo(123);

        ContextAndItem<Integer> copy = new ContextAndItem<>(Context.of("foo", "bar"), 123);
        ContextAndItem<Integer> diff1 = new ContextAndItem<>(Context.of("foo", "baz"), 123);
        ContextAndItem<Integer> diff2 = new ContextAndItem<>(Context.of("foo", "bar"), 456);

        assertThat(copy).isEqualTo(contextAndItem);
        assertThat(diff1).isNotEqualTo(contextAndItem);
        assertThat(diff2).isNotEqualTo(contextAndItem);
    }

}
