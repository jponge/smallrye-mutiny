package io.smallrye.mutiny;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.helpers.BlockingIterable;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;

// TODO
class ContextTest {

    @Nested
    @DisplayName("Context factory methods")
    class ContextFactoryMethods {

        @Test
        void empty() {
            Context context = Context.empty();
            assertThat(context.keys()).isEmpty();
        }

        @Test
        void balancedOf() {
            Context context = Context.of("foo", "bar", "abc", "def");
            assertThat(context.keys())
                    .hasSize(2)
                    .contains("foo", "abc");
        }

        @Test
        void emptyOf() {
            Context context = Context.of();
            assertThat(context.keys()).isEmpty();
        }

        @Test
        void nullOf() {
            assertThatThrownBy(() -> Context.of((Object[]) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The entries array cannot be null");
        }

        @Test
        void unbalancedOf() {
            assertThatThrownBy(() -> Context.of("foo", "bar", "baz"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Arguments must be balanced to form (key, value) pairs");
        }

        @Test
        void from() {
            HashMap<String, String> map = new HashMap<String, String>() {
                {
                    put("foo", "bar");
                    put("abc", "def");
                }
            };

            Context context = Context.from(map);
            assertThat(context.keys())
                    .hasSize(2)
                    .contains("foo", "abc");

            map.put("123", "456");
            assertThat(map).hasSize(3);
            assertThat(context.keys()).hasSize(2);
        }

        @Test
        void fromNull() {
            assertThatThrownBy(() -> Context.from(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("The entries map cannot be null");
        }
    }

    @Nested
    @DisplayName("Context methods")
    class ContextMethods {

        @Test
        void sanityChecks() {
            Context context = Context.of("foo", "bar", "123", 456);

            assertThat(context.keys())
                    .hasSize(2)
                    .contains("foo", "123");

            assertThat(context.contains("foo")).isTrue();
            assertThat(context.contains("bar")).isFalse();
            assertThat(context.isEmpty()).isFalse();

            assertThat(context.<String> get("foo")).isEqualTo("bar");
            assertThat(context.getOrElse("bar", () -> "666")).isEqualTo("666");

            context.put("bar", 123);
            assertThat(context.keys()).hasSize(3);
            assertThat(context.getOrElse("bar", () -> 666)).isEqualTo(123);

            context.delete("foo").delete("123").delete("bar");
            assertThat(context.contains("foo")).isFalse();
            assertThat(context.isEmpty()).isTrue();
            assertThat(context.keys()).isEmpty();
        }

        @Test
        void containsOnEmptyContext() {
            assertThat(Context.empty().contains("foo")).isFalse();
        }

        @Test
        void getOnEmptyContext() {
            assertThatThrownBy(() -> Context.empty().get("foo"))
                    .isInstanceOf(NoSuchElementException.class)
                    .hasMessage("The context is empty");

            assertThat(Context.empty().getOrElse("foo", () -> "bar")).isEqualTo("bar");
        }

        @Test
        void getOnMissingKey() {
            assertThatThrownBy(() -> Context.of("foo", "bar").get("yolo"))
                    .isInstanceOf(NoSuchElementException.class)
                    .hasMessage("The context does not have a value for key yolo");
        }

        @Test
        void keysetIsACopy() {
            Context context = Context.of("foo", "bar", "123", 456);
            Set<String> k1 = context.keys();
            context.put("bar", "baz");
            Set<String> k2 = context.keys();
            assertThat(k1).isNotSameAs(k2);
        }
    }

    @Nested
    @DisplayName("Uni with context")
    class UniAndContext {

        @Test
        void noContextShallBeEmpty() {
            UniAssertSubscriber<String> sub = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            sub.assertCompleted().assertItem("63::yolo");
        }

        @Test
        void transformWithContext() {
            Context context = Context.of("foo", "bar");

            UniAssertSubscriber<String> sub = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("63::bar");
        }

        @Test
        void withContextRejectsNullBuilder() {
            assertThatThrownBy(() -> Uni.createFrom().item(1).withContext(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("`builder` must not be `null`");
        }

        @Test
        void withContextRejectsNullReturningBuilder() {
            UniAssertSubscriber<Object> sub = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> null)
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            sub.assertFailedWith(NullPointerException.class, "The builder function returned null");
        }

        @Test
        void withContextRejectsThrowingBuilder() {
            UniAssertSubscriber<Object> sub = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> {
                        throw new RuntimeException("boom");
                    })
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            sub.assertFailedWith(RuntimeException.class, "boom");
        }

        @Test
        void callbacksPropagateContext() {
            Context context = Context.of("foo", "bar");
            AtomicReference<String> result = new AtomicReference<>();

            Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().with(context, result::set);

            assertThat(result.get())
                    .isNotNull()
                    .isEqualTo("63::bar");
        }

        @Test
        void asCompletableStagePropagateContext() {
            Context context = Context.of("foo", "bar");

            Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().asCompletionStage(context)
                    .whenComplete((str, err) -> {
                        assertThat(err).isNull();
                        assertThat(str).isEqualTo("63::bar");
                    });
        }

        @Test
        void serializedSubscriberDoesPropagateContext() {
            Context context = Context.of("foo", "bar");

            UniAssertSubscriber<String> sub = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().withSerializedSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("63::bar");
        }

        @Test
        void callbacksWithoutContextPropagateEmptyContext() {
            AtomicReference<String> result = new AtomicReference<>();

            Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().with(result::set);

            assertThat(result.get())
                    .isNotNull()
                    .isEqualTo("63::yolo");
        }

        @Test
        void contextDoesPropagate() {
            Context context = Context.of("foo", "bar");

            UniAssertSubscriber<String> sub = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(() -> ctx.put("counter", ctx.<Integer> get("counter") + 1)))
                    .withContext((uni, ctx) -> uni
                            .onItem().invoke(() -> ctx.put("counter", ctx.<Integer> get("counter") + 1)))
                    .withContext((uni, ctx) -> uni
                            .onItem().invoke(() -> ctx.put("counter", ctx.<Integer> get("counter") + 1)))
                    .withContext((uni, ctx) -> uni
                            .onItem().transform(Object::toString)
                            .onItem().transform(s -> s + "::" + ctx.get("counter")))
                    .withContext((uni, ctx) -> {
                        ctx.put("counter", 0);
                        return uni;
                    })
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("63::3");
            assertThat(context.<Integer> get("counter")).isEqualTo(3);
        }

        @Test
        void uniOperatorSubclassesPropagateContext() {
            Context context = Context.of("foo", "bar");

            UniAssertSubscriber<String> sub = Uni.createFrom().failure(new IOException("boom"))
                    .withContext((uni, ctx) -> uni
                            .onItem().transformToUni(obj -> Uni.createFrom().item("Yolo"))
                            .onFailure().recoverWithItem(ctx.<String> get("foo")))
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("bar");
        }

        @Test
        void joinAllAndAttachContext() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            UniAssertSubscriber<String> sub = Uni.join().all(a, b, c).andFailFast()
                    .attachContext()
                    .onItem().transform(itemsWithContext -> {
                        Context ctx = itemsWithContext.context();
                        return itemsWithContext.get().toString() + "::" + ctx.get("58") + "::" + ctx.get("63") + "::"
                                + ctx.get("69");
                    })
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("[58, 63, 69]::58::63::69");
        }

        @Test
        void joinFirstAndAttachContext() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            UniAssertSubscriber<String> sub = Uni.join().first(a, b, c).withItem()
                    .attachContext()
                    .onItem().transform(item -> {
                        Context ctx = item.context();
                        return item.get().toString() + "::" + ctx.get("58");
                    })
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("58::58");
        }

        @Test
        void combineAllAndAttachContext() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            UniAssertSubscriber<String> sub = Uni.combine().all().unis(a, b, c).asTuple()
                    .attachContext()
                    .onItem().transform(itemsWithContext -> {
                        Context ctx = itemsWithContext.context();
                        return itemsWithContext.get().toString() + "::" + ctx.get("58") + "::" + ctx.get("63") + "::"
                                + ctx.get("69");
                    })
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("Tuple{item1=58,item2=63,item3=69}::58::63::69");
        }

        @Test
        void combineAnyAndAttachContext() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Uni<Integer> a = Uni.createFrom().item(58)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> b = Uni.createFrom().item(63)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            Uni<Integer> c = Uni.createFrom().item(69)
                    .withContext((uni, ctx) -> uni.onItem().invoke(n -> ctx.put(n.toString(), n)));

            UniAssertSubscriber<String> sub = Uni.combine().any().of(a, b, c)
                    .attachContext()
                    .onItem().transform(item -> {
                        Context ctx = item.context();
                        return item.get().toString() + "::" + ctx.get("58");
                    })
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("58::58");
        }

        @Test
        void memoization() {
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
        void numberOfCallsToContextMethod() {
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

        @Test
        void blockingAwait() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            String res = Uni.createFrom().item(63)
                    .attachContext()
                    .onItem().transform(item -> item.get() + "::" + item.context().get("foo"))
                    .awaitUsing(context).indefinitely();

            assertThat(res).isEqualTo("63::bar");
        }

        @Test
        void blockingAwaitOptional() {
            Context context = Context.of("foo", "bar", "baz", "baz");

            Optional<String> res = Uni.createFrom().item(63)
                    .attachContext()
                    .onItem().transform(item -> item.get() + "::" + item.context().get("foo"))
                    .awaitUsing(context).asOptional().indefinitely();

            assertThat(res)
                    .isPresent()
                    .hasValue("63::bar");
        }
    }

    @Nested
    @DisplayName("{Uni,Multi} <-> {Multi,Uni} bridges")
    class UniMultiBridges {

        @Test
        void uniToUni() {
            Context context = Context.of("foo", "bar");

            UniAssertSubscriber<String> sub = Uni.createFrom().item("abc")
                    .withContext((uni, ctx) -> uni
                            .onItem().transformToUni(s -> Uni.createFrom().item(s + "::" + ctx.get("foo"))))
                    .onFailure().recoverWithItem(() -> "woops")
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("abc::bar");
        }

        @Test
        void multiToMulti() {
            Context context = Context.of("foo", "bar");

            AssertSubscriber<String> sub = Multi.createFrom().items(1, 2, 3)
                    .withContext((multi, ctx) -> multi
                            .onItem().transformToMultiAndMerge(n -> Multi.createFrom().item(n + "@" + ctx.get("foo"))))
                    .onFailure().recoverWithItem(() -> "woops")
                    .subscribe().withSubscriber(AssertSubscriber.create(context, 10));

            List<String> items = sub.assertCompleted().getItems();
            assertThat(items).hasSize(3)
                    .contains("1@bar", "2@bar", "3@bar");
        }

        @Test
        void uniToMulti() {
            Context context = Context.of("foo", "bar");

            AssertSubscriber<String> sub = Uni.createFrom().item("abc")
                    .withContext((uni, ctx) -> uni.onItem().transform(s -> s + "@" + ctx.get("foo")))
                    .onItem().transformToMulti(s -> Multi.createFrom().item(s))
                    .withContext((multi, ctx) -> multi
                            .onItem().transformToMultiAndConcatenate(s -> Multi.createFrom().items(s, ctx.get("foo"))))
                    .onFailure().recoverWithItem(() -> "woops")
                    .subscribe().withSubscriber(AssertSubscriber.create(context, 10));

            List<String> items = sub.assertCompleted().getItems();
            assertThat(items)
                    .hasSize(2)
                    .contains("abc@bar", "bar");
        }

        @Test
        void multiToUni() {
            Context context = Context.of("foo", "bar");

            UniAssertSubscriber<Object> sub = Multi.createFrom().items(1, 2, 3)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "@" + ctx.get("foo")))
                    .toUni()
                    .withContext((uni, ctx) -> uni
                            .onItem().transformToUni(s -> Uni.createFrom().item(s + "::" + ctx.get("foo"))))
                    .onFailure().recoverWithItem(() -> "woops")
                    .subscribe().withSubscriber(UniAssertSubscriber.create(context));

            sub.assertCompleted().assertItem("1@bar::bar");
        }
    }

    @Nested
    @DisplayName("Uni with context")
    class MultiAndContext {

        @Test
        void noContextShallBeEmpty() {
            AssertSubscriber<String> sub = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().withSubscriber(AssertSubscriber.create(10L));

            sub.assertCompleted();
            assertThat(sub.getItems())
                    .hasSize(3)
                    .containsExactly("58::yolo", "63::yolo", "69::yolo");
        }

        @Test
        void transformWithContext() {
            Context context = Context.of("foo", "bar");

            AssertSubscriber<String> sub = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().withSubscriber(AssertSubscriber.create(context, 10L));

            sub.assertCompleted();
            assertThat(sub.getItems())
                    .hasSize(3)
                    .containsExactly("58::bar", "63::bar", "69::bar");
        }

        @Test
        void withContextRejectsNullBuilder() {
            assertThatThrownBy(() -> Multi.createFrom().item(1).withContext(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("`builder` must not be `null`");
        }

        @Test
        void withContextRejectsNullReturningBuilder() {
            AssertSubscriber<Object> sub = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> null)
                    .subscribe().withSubscriber(AssertSubscriber.create(10L));

            sub.assertFailedWith(NullPointerException.class, "The builder function returned null");
        }

        @Test
        void withContextRejectsThrowingBuilder() {
            AssertSubscriber<Object> sub = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> {
                        throw new RuntimeException("boom");
                    })
                    .subscribe().withSubscriber(AssertSubscriber.create(10L));

            sub.assertFailedWith(RuntimeException.class, "boom");
        }

        @Test
        void callbacksPropagateContext() {
            Context context = Context.of("foo", "bar");
            ArrayList<String> list = new ArrayList<>();

            Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().with(context, list::add);

            assertThat(list)
                    .hasSize(3)
                    .containsExactly("58::bar", "63::bar", "69::bar");
        }

        @Test
        void callbacksWithoutContextPropagateEmptyContext() {
            ArrayList<String> list = new ArrayList<>();

            Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().with(list::add);

            assertThat(list)
                    .hasSize(3)
                    .containsExactly("58::yolo", "63::yolo", "69::yolo");
        }

        @Test
        void asIterableWithoutContext() {
            BlockingIterable<String> iter = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().asIterable();

            assertThat(iter).containsExactly("58::yolo", "63::yolo", "69::yolo");
        }

        @Test
        void asIterableWithContext() {
            BlockingIterable<String> iter = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().asIterable(() -> Context.of("foo", "bar"));

            assertThat(iter).containsExactly("58::bar", "63::bar", "69::bar");
        }

        @Test
        void asStreamWithoutContext() {
            Stream<String> stream = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().asStream();

            assertThat(stream).containsExactly("58::yolo", "63::yolo", "69::yolo");
        }

        @Test
        void asStreamWithContext() {
            Stream<String> stream = Multi.createFrom().items(58, 63, 69)
                    .withContext((multi, ctx) -> multi.onItem().transform(n -> n + "::" + ctx.getOrElse("foo", () -> "yolo")))
                    .subscribe().asStream(() -> Context.of("foo", "bar"));

            assertThat(stream).containsExactly("58::bar", "63::bar", "69::bar");
        }
    }

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
                        System.out.println(contextAndItem.get() + " -> " + contextAndItem.context());
                        return contextAndItem.get();
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
                    .map(cai -> cai.get() + " -> " + cai.context().keys())
                    .collect(Collectors.toList());

            assertThat(list).hasSize(9).contains("6 -> [foo, baz]");
            System.out.println(list);
        }
    }
}
