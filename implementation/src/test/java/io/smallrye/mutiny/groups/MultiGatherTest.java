package io.smallrye.mutiny.groups;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

class MultiGatherTest {

    @Test
    void gatherStringSplit() {
        List<String> items = List.of("123", ",", "abc\n", "f", "oo", ",bar,baz", "\n");

        AssertSubscriber<String> sub = Multi.createFrom().iterable(items)
                .gather(StringBuilder::new, StringBuilder::append, builder -> {
                    String current = builder.toString();
                    String[] splits = current.split("[,\n]", -1);
                    if (splits.length == 1) {
                        return Multi.createFrom().empty();
                    } else {
                        String[] head = new String[splits.length - 1];
                        System.arraycopy(splits, 0, head, 0, head.length);
                        builder.setLength(0);
                        builder.append(splits[splits.length - 1]);
                        return Multi.createFrom().items(head);
                    }
                })
                .subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));

        sub.assertCompleted()
                .assertItems("123", "abc", "foo", "bar", "baz");
    }
}
