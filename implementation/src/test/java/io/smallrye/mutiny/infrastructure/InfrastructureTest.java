package io.smallrye.mutiny.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import junit5.support.InfrastructureResource;

@ResourceLock(InfrastructureResource.NAME)
class InfrastructureTest {

    @AfterEach
    void reset() {
        Infrastructure.reload();
    }

    @Test
    @DisplayName("Ensure Infrastructure.setMultiOverflowDefaultBufferSize rejects 0 buffer sizes")
    void rejectZeroMultiOverflowBufferSizes() {
        IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
                () -> Infrastructure.setMultiOverflowDefaultBufferSize(0));
        assertThat(err).hasMessageContaining("must be greater than zero");
    }

    @Test
    @DisplayName("Ensure Infrastructure.setMultiOverflowDefaultBufferSize rejects negative buffer sizes")
    void rejectNegativeMultiOverflowBufferSizes() {
        IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
                () -> Infrastructure.setMultiOverflowDefaultBufferSize(-100));
        assertThat(err).hasMessageContaining("must be greater than zero");
    }

    @Test
    @DisplayName("Ensure Infrastructure.setMultiOverflowDefaultBufferSize accepts correct buffer sizes")
    void acceptCorrectMultiOverflowBufferSizes() {
        Infrastructure.setMultiOverflowDefaultBufferSize(256);
        assertThat(Infrastructure.getMultiOverflowDefaultBufferSize()).isEqualTo(256);
    }

    @Test
    @DisplayName("Buffer sizes definitions when there are no matching system properties")
    void bufferSizesNoSysProp() {
        assertThat(Infrastructure.getBufferSizeXs()).isEqualTo(32);
        assertThat(Infrastructure.getBufferSizeS()).isEqualTo(256);

        Infrastructure.setBufferSizeXs(4);
        assertThat(Infrastructure.getBufferSizeXs()).isEqualTo(4);

        Infrastructure.setBufferSizeS(4);
        assertThat(Infrastructure.getBufferSizeS()).isEqualTo(4);

        assertThatThrownBy(() -> Infrastructure.setBufferSizeXs(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");

        assertThatThrownBy(() -> Infrastructure.setBufferSizeXs(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");

        assertThatThrownBy(() -> Infrastructure.setBufferSizeS(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");

        assertThatThrownBy(() -> Infrastructure.setBufferSizeS(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
    }

    @Test
    @DisplayName("Buffer sizes definitions when there are matching system properties")
    void bufferSizesWithSysProp() {
        try {
            System.setProperty("mutiny.buffer-size.s", "1024");
            System.setProperty("mutiny.buffer-size.xs", "64");
            Infrastructure.reload();
            assertThat(Infrastructure.getBufferSizeXs()).isEqualTo(64);
            assertThat(Infrastructure.getBufferSizeS()).isEqualTo(1024);
        } finally {
            System.clearProperty("mutiny.buffer-size.s");
            System.clearProperty("mutiny.buffer-size.xs");
        }
        Infrastructure.reload();
        assertThat(Infrastructure.getBufferSizeXs()).isEqualTo(32);
        assertThat(Infrastructure.getBufferSizeS()).isEqualTo(256);
    }

    @Test
    @DisplayName("configure() sets the default executor")
    void configureSetsExecutor() {
        Executor executor = Runnable::run;
        Infrastructure.configure(new InfrastructureConfiguration()
                .withDefaultExecutor(executor));
        assertThat(Infrastructure.getDefaultExecutor()).isSameAs(executor);
    }

    @Test
    @DisplayName("configure() applies uni interceptors during pipeline assembly")
    void configureAppliesUniInterceptors() {
        AtomicBoolean called = new AtomicBoolean();
        UniInterceptor interceptor = new UniInterceptor() {
            @Override
            public <T> Uni<T> onUniCreation(Uni<T> uni) {
                called.set(true);
                return uni;
            }
        };
        Infrastructure.configure(new InfrastructureConfiguration()
                .withUniInterceptors(List.of(interceptor)));
        Uni.createFrom().item(1);
        assertThat(called).isTrue();
    }

    @Test
    @DisplayName("configure() applies multi interceptors during pipeline assembly")
    void configureAppliesMultiInterceptors() {
        AtomicBoolean called = new AtomicBoolean();
        MultiInterceptor interceptor = new MultiInterceptor() {
            @Override
            public <T> Multi<T> onMultiCreation(Multi<T> multi) {
                called.set(true);
                return multi;
            }
        };
        Infrastructure.configure(new InfrastructureConfiguration()
                .withMultiInterceptors(List.of(interceptor)));
        Multi.createFrom().item(1);
        assertThat(called).isTrue();
    }

    @Test
    @DisplayName("configure() rejects null config")
    void configureRejectsNull() {
        assertThatThrownBy(() -> Infrastructure.configure(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("InfrastructureConfiguration validates parameters")
    void configurationValidation() {
        assertThatThrownBy(() -> new InfrastructureConfiguration().withDefaultExecutor(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InfrastructureConfiguration().withUniInterceptors(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InfrastructureConfiguration().withBufferSizeXs(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InfrastructureConfiguration().withBufferSizeS(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("configure() does not shut down the previous executor")
    void configureDoesNotShutDownPreviousExecutor() {
        ExecutorService previous = Executors.newCachedThreadPool();
        try {
            Infrastructure.setDefaultExecutor(previous, false);
            Infrastructure.configure(new InfrastructureConfiguration()
                    .withDefaultExecutor(Runnable::run));
            assertThat(previous.isShutdown()).isFalse();
        } finally {
            previous.shutdownNow();
        }
    }

    @Test
    @DisplayName("Malformed buffer size system property throws NumberFormatException")
    void malformedBufferSizePropertyThrows() {
        try {
            System.setProperty("mutiny.buffer-size.xs", "not-a-number");
            assertThatThrownBy(Infrastructure::reload)
                    .isInstanceOf(NumberFormatException.class);
        } finally {
            System.clearProperty("mutiny.buffer-size.xs");
        }
    }

    @Test
    @DisplayName("configure() sets buffer sizes")
    void configureSetsBufferSizes() {
        Infrastructure.configure(new InfrastructureConfiguration()
                .withBufferSizeXs(64)
                .withBufferSizeS(512)
                .withMultiOverflowDefaultBufferSize(1024));
        assertThat(Infrastructure.getBufferSizeXs()).isEqualTo(64);
        assertThat(Infrastructure.getBufferSizeS()).isEqualTo(512);
        assertThat(Infrastructure.getMultiOverflowDefaultBufferSize()).isEqualTo(1024);
    }

    @Test
    @DisplayName("configure() sets dropped exception handler")
    void configureSetsDroppedExceptionHandler() {
        AtomicBoolean called = new AtomicBoolean();
        Infrastructure.configure(new InfrastructureConfiguration()
                .withDroppedExceptionHandler(t -> called.set(true)));
        Infrastructure.handleDroppedException(new RuntimeException("test"));
        assertThat(called).isTrue();
    }

    @Test
    @DisplayName("configure() sets canCallerThreadBeBlocked supplier")
    void configureSetsCanCallerThreadBeBlockedSupplier() {
        Infrastructure.configure(new InfrastructureConfiguration()
                .withCanCallerThreadBeBlockedSupplier(() -> false));
        assertThat(Infrastructure.canCallerThreadBeBlocked()).isFalse();
    }
}
