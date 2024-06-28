package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class MultiGatherTest {

    @Test
    void stringAppenderGather() {
        List<String> chunks = List.of("foo", ",", "b", "ar,", "b", "a", "z", ",baz\n");
        AssertSubscriber<String> sub = Multi.createFrom().iterable(chunks)
                .gather()
                .toState(StringBuilder::new)
                .withAppender((builder, chunk) -> {
                    builder.append(chunk);
                    return builder;
                })
                .withExtractor(builder -> {
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
                }).subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE));
        sub.assertCompleted().assertItems("foo", "bar", "baz", "baz");
    }

    @Test
    void api_design() {
        Multi.createFrom().range(1, 20)
                .gather().toState(AtomicInteger::new)
                .withAppender((acc, n) -> {
                    acc.addAndGet(n);
                    return acc;
                })
                .withExtractor((s) -> {
                    if (s.get() % 2 == 0) {
                        return Multi.createFrom().item("[" + s.get() + "]");
                    } else {
                        return Multi.createFrom().empty();
                    }
                }).subscribe().with(System.out::println);

        Multi.createFrom().range(1, 20)
                .gather().toStream(() -> {
                    BroadcastProcessor<String> processor = BroadcastProcessor.create();
                    return new MultiGather.MultiAndAppender<>(processor, n -> {
                        if (n % 2 == 0) {
                            processor.onNext("[" + n + "]");
                        }
                    });
                }).subscribe().with(System.out::println);
    }
}