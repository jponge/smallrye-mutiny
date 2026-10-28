package io.smallrye.mutiny.infrastructure;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Configuration holder for {@link Infrastructure#configure(InfrastructureConfiguration)}.
 */
public final class InfrastructureConfiguration {

    private Executor defaultExecutor;
    private List<UniInterceptor> uniInterceptors;
    private List<MultiInterceptor> multiInterceptors;
    private List<CallbackDecorator> callbackDecorators;
    private Consumer<Throwable> droppedExceptionHandler;
    private BooleanSupplier canCallerThreadBeBlockedSupplier;
    private Infrastructure.OperatorLogger operatorLogger;
    private UnaryOperator<CompletableFuture<?>> completableFutureWrapper;
    private int bufferSizeXs;
    private int bufferSizeS;
    private int multiOverflowDefaultBufferSize;

    public Executor getDefaultExecutor() {
        return defaultExecutor;
    }

    public InfrastructureConfiguration withDefaultExecutor(Executor defaultExecutor) {
        this.defaultExecutor = nonNull(defaultExecutor, "defaultExecutor");
        return this;
    }

    public List<UniInterceptor> getUniInterceptors() {
        return uniInterceptors;
    }

    public InfrastructureConfiguration withUniInterceptors(List<UniInterceptor> uniInterceptors) {
        this.uniInterceptors = nonNull(uniInterceptors, "uniInterceptors");
        return this;
    }

    public List<MultiInterceptor> getMultiInterceptors() {
        return multiInterceptors;
    }

    public InfrastructureConfiguration withMultiInterceptors(List<MultiInterceptor> multiInterceptors) {
        this.multiInterceptors = nonNull(multiInterceptors, "multiInterceptors");
        return this;
    }

    public List<CallbackDecorator> getCallbackDecorators() {
        return callbackDecorators;
    }

    public InfrastructureConfiguration withCallbackDecorators(List<CallbackDecorator> callbackDecorators) {
        this.callbackDecorators = nonNull(callbackDecorators, "callbackDecorators");
        return this;
    }

    public Consumer<Throwable> getDroppedExceptionHandler() {
        return droppedExceptionHandler;
    }

    public InfrastructureConfiguration withDroppedExceptionHandler(Consumer<Throwable> droppedExceptionHandler) {
        this.droppedExceptionHandler = nonNull(droppedExceptionHandler, "droppedExceptionHandler");
        return this;
    }

    public BooleanSupplier getCanCallerThreadBeBlockedSupplier() {
        return canCallerThreadBeBlockedSupplier;
    }

    public InfrastructureConfiguration withCanCallerThreadBeBlockedSupplier(BooleanSupplier canCallerThreadBeBlockedSupplier) {
        this.canCallerThreadBeBlockedSupplier = nonNull(canCallerThreadBeBlockedSupplier, "canCallerThreadBeBlockedSupplier");
        return this;
    }

    public Infrastructure.OperatorLogger getOperatorLogger() {
        return operatorLogger;
    }

    public InfrastructureConfiguration withOperatorLogger(Infrastructure.OperatorLogger operatorLogger) {
        this.operatorLogger = nonNull(operatorLogger, "operatorLogger");
        return this;
    }

    public UnaryOperator<CompletableFuture<?>> getCompletableFutureWrapper() {
        return completableFutureWrapper;
    }

    public InfrastructureConfiguration withCompletableFutureWrapper(
            UnaryOperator<CompletableFuture<?>> completableFutureWrapper) {
        this.completableFutureWrapper = nonNull(completableFutureWrapper, "completableFutureWrapper");
        return this;
    }

    public int getBufferSizeXs() {
        return bufferSizeXs;
    }

    public InfrastructureConfiguration withBufferSizeXs(int bufferSizeXs) {
        this.bufferSizeXs = positive(bufferSizeXs, "bufferSizeXs");
        return this;
    }

    public int getBufferSizeS() {
        return bufferSizeS;
    }

    public InfrastructureConfiguration withBufferSizeS(int bufferSizeS) {
        this.bufferSizeS = positive(bufferSizeS, "bufferSizeS");
        return this;
    }

    public int getMultiOverflowDefaultBufferSize() {
        return multiOverflowDefaultBufferSize;
    }

    public InfrastructureConfiguration withMultiOverflowDefaultBufferSize(int multiOverflowDefaultBufferSize) {
        this.multiOverflowDefaultBufferSize = positive(multiOverflowDefaultBufferSize, "multiOverflowDefaultBufferSize");
        return this;
    }
}
