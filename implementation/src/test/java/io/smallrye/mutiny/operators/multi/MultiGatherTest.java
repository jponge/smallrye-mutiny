package io.smallrye.mutiny.operators.multi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.tuples.Tuple2;

class MultiGatherTest {

    @Test
    void gatherToLists() {
        AssertSubscriber<ArrayList<Integer>> sub = Multi.createFrom().range(1, 100)
                .onItem().gather()
                .into(ArrayList<Integer>::new)
                .accumulate((list, next) -> {
                    list.add(next);
                    return list;
                })
                .extract(list -> list.size() > 5
                        ? Optional.of(Tuple2.of(new ArrayList<>(), list))
                        : Optional.empty())
                .finalize(list -> list.isEmpty()
                        ? Optional.empty()
                        : Optional.of(list))
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.awaitNextItems(2);
        assertThat(sub.getItems()).hasSize(2)
                .anySatisfy(list -> assertThat(list).containsExactly(1, 2, 3, 4, 5, 6))
                .anySatisfy(list -> assertThat(list).containsExactly(7, 8, 9, 10, 11, 12));

        sub.request(Long.MAX_VALUE);
        sub.awaitCompletion();
        assertThat(sub.getItems()).hasSize(17)
                .anySatisfy(list -> assertThat(list).containsExactly(91, 92, 93, 94, 95, 96))
                .anySatisfy(list -> assertThat(list).containsExactly(97, 98, 99));

    }

    @Test
    void gatherLinesOfText() {
        List<String> chunks = List.of(
                "Hello", " ", "world!\n",
                "This is a test\n",
                "==\n==",
                "\n\nThis", " is", " ", "amazing\n\n");
        AssertSubscriber<String> sub = Multi.createFrom().iterable(chunks)
                .onItem().gather()
                .into(StringBuilder::new)
                .accumulate(StringBuilder::append)
                .extract(sb -> {
                    String str = sb.toString();
                    if (str.contains("\n")) {
                        String[] lines = str.split("\n", 2);
                        return Optional.of(Tuple2.of(new StringBuilder(lines[1]), lines[0]));
                    }
                    return Optional.empty();
                })
                .finalize(sb -> Optional.of(sb.toString()))
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.awaitNextItems(2);
        assertThat(sub.getItems()).containsExactly("Hello world!", "This is a test");

        sub.request(Long.MAX_VALUE);
        assertThat(sub.getItems()).containsExactly(
                "Hello world!",
                "This is a test",
                "==",
                "==",
                "",
                "This is amazing",
                "",
                "");
    }

    @Test
    void checkCompletionCorrectness() {
        List<String> chunks = List.of(
                "a", "1,b1,c1,d1"
        );
        AssertSubscriber<String> sub = Multi.createFrom().iterable(chunks)
                .onItem().gather()
                    .into(StringBuilder::new)
                    .accumulate(StringBuilder::append)
                    .extract(sb -> {
                        String str = sb.toString();
                        if (str.contains(",")) {
                            String[] lines = str.split(",", 2);
                            return Optional.of(Tuple2.of(new StringBuilder(lines[1]), lines[0]));
                        }
                        return Optional.empty();
                    })
                    .finalize(sb -> Optional.of(sb.toString()))
                .subscribe().withSubscriber(AssertSubscriber.create());

        sub.awaitNextItem().assertNotTerminated();
        assertThat(sub.getItems()).containsExactly("a1");

        sub.awaitNextItem().assertNotTerminated();
        assertThat(sub.getItems()).containsExactly("a1", "b1");

        sub.awaitNextItem().assertNotTerminated();
        assertThat(sub.getItems()).containsExactly("a1", "b1", "c1");

        sub.awaitNextItem().assertCompleted();
        assertThat(sub.getItems()).containsExactly("a1", "b1", "c1", "d1");
    }
}
