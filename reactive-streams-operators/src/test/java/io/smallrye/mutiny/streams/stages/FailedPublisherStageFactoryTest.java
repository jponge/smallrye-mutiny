package io.smallrye.mutiny.streams.stages;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.streams.operators.PublisherStage;
import io.smallrye.mutiny.test.AssertSubscriber;

/**
 * Checks the behavior of {@link FailedPublisherStageFactory}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FailedPublisherStageFactoryTest extends StageTestBase {

    private final FailedPublisherStageFactory factory = new FailedPublisherStageFactory();

    @Test
    public void createWithError() {
        Exception failure = new Exception("Boom");
        PublisherStage<Object> boom = factory.create(null, () -> failure);
        boom.get().subscribe().withSubscriber(AssertSubscriber.create())
                .assertHasFailedWith(Exception.class, "Boom");
    }

    @Test
    public void createWithoutStage() {
        assertThrows(NullPointerException.class, () -> factory.create(null, null));
    }

    @Test
    public void createWithoutError() {
        assertThrows(NullPointerException.class, () -> factory.create(null, () -> null));
    }

}
