package io.smallrye.mutiny;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + " :: " + ctx.get("abc") + " :: " + ctx.get("def") + " :: " + ctx.get("foo")))
                    .subscribe().with(context, System.out::println);

            System.out.println(context);
        }

        @Test
        void smoke2() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<? extends Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<? extends Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<? extends Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni.join().all(a, b, c).andFailFast()
                    .withContext((uni, ctx) -> uni.replaceWith(() -> ctx.get("58") + " :: " + ctx.get("63") + " :: " + ctx.get("69")))
                    .subscribe().with(context, System.out::println);

            System.out.println(context);
        }

        @Test
        void smoke3() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<? extends Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<? extends Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<? extends Integer> c = Uni.createFrom().item(69)
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
    }
}
