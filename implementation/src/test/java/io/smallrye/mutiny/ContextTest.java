package io.smallrye.mutiny;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;

// TODO
class ContextTest {

    @Nested
    @DisplayName("Tests in progress")
    class InProgress {

        @Test
        void smoke() {
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

            Uni<? extends Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<? extends Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<? extends Integer> c = Uni.createFrom().item(69)
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
    }
}
