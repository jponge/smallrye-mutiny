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
                    .withContext((uni, ctx) -> uni.onItem().transform(n -> n + " :: " + ctx.get("abc") + " :: " + ctx.get("def")))
                    .subscribe().with(context, System.out::println);


//                        Uni.createFrom().item(58)
//                                .onItem().updateContext((updater, n) -> updater.put("original", n))
//                                .onItem().transformToUni(n -> Uni.createFrom().item(63))
//                                .onItem().transform(Objects::toString)
//                                .onItem().updateContext((updater, str) -> updater.put("yolo_1", str).put("yolo_2", str + "!"))
//                                .subscribe().with(context, System.out::println);

            System.out.println(context);
        }
    }
}
