package io.smallrye.mutiny;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;

// TODO
class ContextTest {

    @Nested
    @DisplayName("Multi smoke tests (in progress)")
    class MultiInProgress {

        @Test
        void smoke1() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            AssertSubscriber<String> sub = Multi.createFrom().range(1, 10)
                    .withContext((multi, ctx) -> {
                        System.out.println(ctx);
                        return multi.onItem().transform(n -> {
                            ctx.put("count", ctx.getOrElse("count", () -> 0) + 1);
                            return n + " :: " + ctx.getOrElse("foo", () -> "!!!") + " @" + ctx.get("count");
                        });
                    })
                    .attachContext().onItem().transform(contextAndItem -> {
                        System.out.println(contextAndItem.item() + " -> " + contextAndItem.context());
                        return contextAndItem.item();
                    })
                    .subscribe().withSubscriber(AssertSubscriber.create(context, Long.MAX_VALUE));

            System.out.println(sub.getItems());
            sub.assertCompleted();
            assertThat(sub.getItems()).contains("2 :: bar @2", "7 :: bar @7");
        }

        @Test
        void smoke2() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            AssertSubscriber<String> sub = Multi.createFrom().range(1, 10)
                    .select().where(n -> n % 2 == 0)
                    .onItem().transform(n -> n * 10)
                    .withContext((multi, ctx) -> multi.onItem().transformToUniAndMerge(n -> {
                        ctx.put("count", ctx.getOrElse("count", () -> 0) + 1);
                        return Uni.createFrom().item(n).withContext((uni, ctx2) -> uni.onItem()
                                .transform(m -> m + " :: " + ctx2.getOrElse("foo", () -> "!!!") + " @" + ctx2.get("count")));
                    }))
                    .subscribe().withSubscriber(AssertSubscriber.create(context, Long.MAX_VALUE));

            System.out.println(sub.getItems());
            sub.assertCompleted();
            assertThat(sub.getItems()).contains("20 :: bar @1", "80 :: bar @4");
        }

        @Test
        void smoke3() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            AssertSubscriber<String> sub = Multi.createFrom().range(1, 10)
                    .withContext((multi, ctx) -> multi.onItem().transformToMultiAndMerge(
                            n -> Multi.createFrom().items(n.toString(), ctx.get("foo"), ctx.get("baz"))))
                    .onFailure().retry().atMost(5)
                    .subscribe().withSubscriber(AssertSubscriber.create(context, Long.MAX_VALUE));

            System.out.println(sub.getItems());
            sub.assertCompleted();
        }

        @Test
        void smoke4() {
            List<String> list = Multi.createFrom().range(1, 10)
                    .attachContext()
                    .subscribe().asStream(() -> Context.of("foo", "bar", "baz", "baz"))
                    .map(cai -> cai.item() + " -> " + cai.context().keys())
                    .collect(Collectors.toList());

            assertThat(list).hasSize(9).contains("6 -> [foo, baz]");
            System.out.println(list);
        }
    }

    @Nested
    @DisplayName("Uni smoke tests (in progress)")
    class UniInProgress {

        @Test
        void smoke1() {
            Context context = Context.of("abc", 123, "def", true);

            Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> {
                        ctx.put("foo", "bar");
                        return uni;
                    })
                    .withContext((uni, ctx) -> uni.onItem()
                            .transform(n -> n + " :: " + ctx.get("abc") + " :: " + ctx.get("def") + " :: " + ctx.get("foo")))
                    .subscribe().with(context, System.out::println);

            System.out.println(context);
        }

        @Test
        void smoke2() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni.join().all(a, b, c).andFailFast()
                    .withContext((uni, ctx) -> uni
                            .replaceWith(() -> ctx.get("58") + " :: " + ctx.get("63") + " :: " + ctx.get("69")))
                    .subscribe().with(context, System.out::println);

            System.out.println(context);
        }

        @Test
        void smoke3() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni.join().all(a, b, c).andFailFast()
                    .attachContext()
                    .onItem().transform(contextAndItem -> {
                        Context ctx = contextAndItem.context();
                        return contextAndItem.item() + " => " + ctx.get("58") + " :: " + ctx.get("63") + " :: " + ctx.get("69");
                    })
                    .subscribe().with(context, System.out::println);

            System.out.println(context);
        }

        @Test
        void smoke4() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            UniAssertSubscriber<String> pipeline = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> {
                        ctx.put("abc", 123);
                        return uni;
                    })
                    .onItem().transform(Object::toString)
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            assertThat(context.contains("abc")).isTrue();
            assertThat(context.contains("foo")).isTrue();
            assertThat(context.<Integer> get("abc")).isEqualTo(123);
            assertThat(context.<String> get("foo")).isEqualTo("bar");

            pipeline.assertCompleted().assertItem("63");
        }

        @Test
        void smoke5() {
            AtomicBoolean invalidator = new AtomicBoolean(false);

            Context firstContext = Context.of("foo", "bar", "baz", "baz");

            Uni<String> pipeline = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> {
                        ctx.put("abc", 123);
                        return uni;
                    })
                    .onItem().transform(Object::toString)
                    .memoize().until(invalidator::get);

            UniAssertSubscriber<String> sub = pipeline.subscribe().withSubscriber(UniAssertSubscriber.create(firstContext));

            assertThat(firstContext.contains("abc")).isTrue();
            assertThat(firstContext.contains("foo")).isTrue();
            assertThat(firstContext.<Integer> get("abc")).isEqualTo(123);
            assertThat(firstContext.<String> get("foo")).isEqualTo("bar");
            sub.assertCompleted().assertItem("63");

            Context secondContext = Context.empty();
            sub = pipeline.subscribe().withSubscriber(UniAssertSubscriber.create(secondContext));

            sub.assertCompleted().assertItem("63");
            assertThat(secondContext.isEmpty()).isTrue();

            invalidator.set(true);

            Context thirdContext = Context.empty();
            sub = pipeline.subscribe().withSubscriber(UniAssertSubscriber.create(thirdContext));

            sub.assertCompleted().assertItem("63");
            assertThat(thirdContext.<Integer> get("abc")).isEqualTo(123);
            assertThat(thirdContext.contains("foo")).isFalse();
        }

        @Test
        void smoke6() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni.combine().all().unis(a, b, c).asTuple()
                    .attachContext()
                    .onItem().transform(contextAndItem -> {
                        Context ctx = contextAndItem.context();
                        return contextAndItem.item() + " => " + ctx.get("58") + " :: " + ctx.get("63") + " :: " + ctx.get("69");
                    })
                    .subscribe().with(context, System.out::println);

            System.out.println(context);
        }

        @Test
        void smoke7() {
            Context context = Context.of("foo", "bar", "baz", "baz");
            AtomicInteger counter = new AtomicInteger();

            UniAssertSubscriber<Integer> sub = Uni.createFrom().item(1)
                    .withContext((uni, ctx) -> uni
                            .onItem().transform(n -> n + "!")
                            .onItem().transform(s -> "[" + s + "]"))
                    .onItem().transform(String::toUpperCase)
                    .onItem().transform(String::length)
                    .withContext((uni, ctx) -> uni)
                    .subscribe().withSubscriber(new UniAssertSubscriber<Integer>() {
                        @Override
                        public Context context() {
                            counter.incrementAndGet();
                            return context;
                        }
                    });

            sub.assertCompleted().assertItem(4);
            assertThat(counter.get()).isEqualTo(2);
        }
    }
}
