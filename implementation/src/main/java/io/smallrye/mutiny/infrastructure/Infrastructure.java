package io.smallrye.mutiny.infrastructure;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;
import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.MultiOverflowStrategy;
import io.smallrye.mutiny.subscription.UniSubscriber;
import io.smallrye.mutiny.tuples.Functions;

public class Infrastructure {

    private static final String DISABLE_CALLBACK_DECORATORS_PROP_NAME = "mutiny.disableCallBackDecorators";
    private static final boolean DISABLE_CALLBACK_DECORATORS = Boolean.getBoolean(DISABLE_CALLBACK_DECORATORS_PROP_NAME);

    static {
        resetCanCallerThreadBeBlockedSupplier();
    }

    private static ScheduledExecutorService DEFAULT_SCHEDULER;

    private static Executor DEFAULT_EXECUTOR;
    private static UniInterceptor[] UNI_INTERCEPTORS;
    private static MultiInterceptor[] MULTI_INTERCEPTORS;
    private static CallbackDecorator[] CALLBACK_DECORATORS;
    private static UnaryOperator<CompletableFuture<?>> completableFutureWrapper;
    private static Consumer<Throwable> droppedExceptionHandler = PrintAndDumpThrowableConsumer.INSTANCE;
    private static BooleanSupplier canCallerThreadBeBlockedSupplier;
    private static OperatorLogger operatorLogger = PrintOperatorEventOperatorLogger.INSTANCE;

    private static int multiOverflowDefaultBufferSize = 128;

    private static int bufferSizeXs = resolveBufferSize("mutiny.buffer-size.xs", 32, 8);
    private static int bufferSizeS = resolveBufferSize("mutiny.buffer-size.s", 256, 16);

    public static void reload() {
        ServiceLoader<ExecutorConfiguration> executorLoader = ServiceLoader.load(ExecutorConfiguration.class);
        Iterator<ExecutorConfiguration> iterator = executorLoader.iterator();
        if (iterator.hasNext()) {
            ExecutorConfiguration next = iterator.next();
            setDefaultExecutor(nonNull(next.getDefaultWorkerExecutor(), "executor"));
        } else {
            setDefaultExecutor();
        }
        reloadUniInterceptors();
        reloadMultiInterceptors();
        reloadCallbackDecorators();
        resetCanCallerThreadBeBlockedSupplier();
        multiOverflowDefaultBufferSize = 128;
        bufferSizeXs = resolveBufferSize("mutiny.buffer-size.xs", 32, 8);
        bufferSizeS = resolveBufferSize("mutiny.buffer-size.s", 256, 16);
    }

    public static void configure(InfrastructureConfiguration config) {
        nonNull(config, "config");
        if (config.getDefaultExecutor() != null) {
            setDefaultExecutor(config.getDefaultExecutor(), false);
        }
        if (config.getUniInterceptors() != null) {
            UNI_INTERCEPTORS = config.getUniInterceptors().toArray(new UniInterceptor[0]);
        }
        if (config.getMultiInterceptors() != null) {
            MULTI_INTERCEPTORS = config.getMultiInterceptors().toArray(new MultiInterceptor[0]);
        }
        if (config.getCallbackDecorators() != null) {
            CALLBACK_DECORATORS = config.getCallbackDecorators().toArray(new CallbackDecorator[0]);
        }
        if (config.getDroppedExceptionHandler() != null) {
            droppedExceptionHandler = config.getDroppedExceptionHandler();
        }
        if (config.getCanCallerThreadBeBlockedSupplier() != null) {
            canCallerThreadBeBlockedSupplier = config.getCanCallerThreadBeBlockedSupplier();
        }
        if (config.getOperatorLogger() != null) {
            operatorLogger = config.getOperatorLogger();
        }
        if (config.getCompletableFutureWrapper() != null) {
            completableFutureWrapper = config.getCompletableFutureWrapper();
        }
        if (config.getBufferSizeXs() > 0) {
            bufferSizeXs = config.getBufferSizeXs();
        }
        if (config.getBufferSizeS() > 0) {
            bufferSizeS = config.getBufferSizeS();
        }
        if (config.getMultiOverflowDefaultBufferSize() > 0) {
            multiOverflowDefaultBufferSize = config.getMultiOverflowDefaultBufferSize();
        }
    }

    /**
     * Configure or reset the executors.
     */
    public static void setDefaultExecutor() {
        ExecutorService scheduler = Executors.newCachedThreadPool();
        setDefaultExecutor(scheduler);
    }

    /**
     * Changes the default executor, and shuts the previous executor down.
     *
     * @param s the new executor
     */
    public static void setDefaultExecutor(Executor s) {
        setDefaultExecutor(s, true);
    }

    /**
     * Changes the default executor, and shuts the previous executor down when {@code shutdownPrevious} is {@code true}.
     *
     * @param executor the new executor
     * @param shutdownPrevious {@code true} when the previous executor needs to be shut down, {@code false} otherwise
     */
    public static void setDefaultExecutor(Executor executor, boolean shutdownPrevious) {
        if (executor == DEFAULT_EXECUTOR) {
            return;
        }
        if (shutdownPrevious) {
            Executor existing = DEFAULT_EXECUTOR;
            if (existing instanceof ExecutorService) {
                ((ExecutorService) existing).shutdownNow();
            }
            if (DEFAULT_SCHEDULER != null) {
                DEFAULT_SCHEDULER.shutdownNow();
            }
        }
        DEFAULT_EXECUTOR = executor;
        DEFAULT_SCHEDULER = (executor instanceof ScheduledExecutorService) ? (ScheduledExecutorService) executor
                : new MutinyScheduler(executor);
    }

    public static ScheduledExecutorService getDefaultWorkerPool() {
        if (DEFAULT_SCHEDULER == null) {
            getDefaultExecutor();
        }
        return DEFAULT_SCHEDULER;
    }

    public static Executor getDefaultExecutor() {
        if (DEFAULT_EXECUTOR == null) {
            ServiceLoader<ExecutorConfiguration> loader = ServiceLoader.load(ExecutorConfiguration.class);
            Iterator<ExecutorConfiguration> iterator = loader.iterator();
            if (iterator.hasNext()) {
                setDefaultExecutor(nonNull(iterator.next().getDefaultWorkerExecutor(), "executor"));
            } else {
                setDefaultExecutor();
            }
        }
        return DEFAULT_EXECUTOR;
    }

    private static UniInterceptor[] uniInterceptors() {
        if (UNI_INTERCEPTORS == null) {
            reloadUniInterceptors();
        }
        return UNI_INTERCEPTORS;
    }

    private static MultiInterceptor[] multiInterceptors() {
        if (MULTI_INTERCEPTORS == null) {
            reloadMultiInterceptors();
        }
        return MULTI_INTERCEPTORS;
    }

    private static CallbackDecorator[] callbackDecorators() {
        if (CALLBACK_DECORATORS == null) {
            reloadCallbackDecorators();
        }
        return CALLBACK_DECORATORS;
    }

    public static <T> Uni<T> onUniCreation(Uni<T> instance) {
        Uni<T> current = instance;
        for (UniInterceptor interceptor : uniInterceptors()) {
            current = interceptor.onUniCreation(current);
        }
        return current;
    }

    public static <T> Multi<T> onMultiCreation(Multi<T> instance) {
        Multi<T> current = instance;
        for (MultiInterceptor interceptor : multiInterceptors()) {
            current = interceptor.onMultiCreation(current);
        }
        return current;
    }

    public static <T> UniSubscriber<? super T> onUniSubscription(Uni<T> instance, UniSubscriber<? super T> subscriber) {
        UniSubscriber<? super T> current = subscriber;
        for (UniInterceptor interceptor : uniInterceptors()) {
            current = interceptor.onSubscription(instance, current);
        }
        return current;
    }

    public static <T> Subscriber<? super T> onMultiSubscription(Flow.Publisher<? extends T> instance,
            Subscriber<? super T> subscriber) {
        Subscriber<? super T> current = subscriber;
        for (MultiInterceptor interceptor : multiInterceptors()) {
            current = interceptor.onSubscription(instance, current);
        }
        return current;
    }

    public static <T> Supplier<T> decorate(Supplier<T> supplier) {
        Supplier<T> current = supplier;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <T> Consumer<T> decorate(Consumer<T> consumer) {
        Consumer<T> current = consumer;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static LongConsumer decorate(LongConsumer consumer) {
        LongConsumer current = consumer;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <R> LongFunction<R> decorate(LongFunction<R> function) {
        LongFunction<R> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static Runnable decorate(Runnable runnable) {
        Runnable current = runnable;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <V> Callable<V> decorate(Callable<V> callable) {
        Callable<V> current = callable;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <T1, T2> BiConsumer<T1, T2> decorate(BiConsumer<T1, T2> consumer) {
        BiConsumer<T1, T2> current = consumer;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, O> Functions.Function3<I1, I2, I3, O> decorate(Functions.Function3<I1, I2, I3, O> function) {
        Functions.Function3<I1, I2, I3, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, I4, O> Functions.Function4<I1, I2, I3, I4, O> decorate(
            Functions.Function4<I1, I2, I3, I4, O> function) {
        Functions.Function4<I1, I2, I3, I4, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, I4, I5, O> Functions.Function5<I1, I2, I3, I4, I5, O> decorate(
            Functions.Function5<I1, I2, I3, I4, I5, O> function) {
        Functions.Function5<I1, I2, I3, I4, I5, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, I4, I5, I6, O> Functions.Function6<I1, I2, I3, I4, I5, I6, O> decorate(
            Functions.Function6<I1, I2, I3, I4, I5, I6, O> function) {
        Functions.Function6<I1, I2, I3, I4, I5, I6, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, I4, I5, I6, I7, O> Functions.Function7<I1, I2, I3, I4, I5, I6, I7, O> decorate(
            Functions.Function7<I1, I2, I3, I4, I5, I6, I7, O> function) {
        Functions.Function7<I1, I2, I3, I4, I5, I6, I7, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, I4, I5, I6, I7, I8, O> Functions.Function8<I1, I2, I3, I4, I5, I6, I7, I8, O> decorate(
            Functions.Function8<I1, I2, I3, I4, I5, I6, I7, I8, O> function) {
        Functions.Function8<I1, I2, I3, I4, I5, I6, I7, I8, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, I3, I4, I5, I6, I7, I8, I9, O> Functions.Function9<I1, I2, I3, I4, I5, I6, I7, I8, I9, O> decorate(
            Functions.Function9<I1, I2, I3, I4, I5, I6, I7, I8, I9, O> function) {
        Functions.Function9<I1, I2, I3, I4, I5, I6, I7, I8, I9, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I, O> Function<I, O> decorate(Function<I, O> function) {
        Function<I, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <I1, I2, O> BiFunction<I1, I2, O> decorate(BiFunction<I1, I2, O> function) {
        BiFunction<I1, I2, O> current = function;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <T> BinaryOperator<T> decorate(BinaryOperator<T> operator) {
        BinaryOperator<T> current = operator;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <T1, T2, T3> Functions.TriConsumer<T1, T2, T3> decorate(
            Functions.TriConsumer<T1, T2, T3> consumer) {
        Functions.TriConsumer<T1, T2, T3> current = consumer;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static void setCompletableFutureWrapper(UnaryOperator<CompletableFuture<?>> wrapper) {
        completableFutureWrapper = wrapper;
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> wrapCompletableFuture(CompletableFuture<T> future) {
        UnaryOperator<CompletableFuture<?>> wrapper = completableFutureWrapper;
        return wrapper != null ? (CompletableFuture<T>) wrapper.apply(future) : future;
    }

    public static void handleDroppedException(Throwable throwable) {
        droppedExceptionHandler.accept(throwable);
    }

    /**
     * Defines a custom caller thread blocking check supplier.
     *
     * @param supplier the supplier, must not be {@code null} and must not throw an exception or it will also be lost.
     */
    public static void setCanCallerThreadBeBlockedSupplier(BooleanSupplier supplier) {
        nonNull(supplier, "supplier");
        canCallerThreadBeBlockedSupplier = supplier;
    }

    public static boolean canCallerThreadBeBlocked() {
        return canCallerThreadBeBlockedSupplier.getAsBoolean();
    }

    /**
     * Defines a custom dropped exception handler.
     *
     * @param handler the handler, must not be {@code null} and must not throw an exception or it will also be lost.
     */
    public static void setDroppedExceptionHandler(Consumer<Throwable> handler) {
        nonNull(handler, "handler");
        droppedExceptionHandler = handler;
    }

    public static void reloadUniInterceptors() {
        ServiceLoader<UniInterceptor> loader = ServiceLoader.load(UniInterceptor.class);
        List<UniInterceptor> interceptors = toInterceptorList(loader);
        UNI_INTERCEPTORS = interceptors.toArray(new UniInterceptor[0]);
    }

    public static void reloadMultiInterceptors() {
        ServiceLoader<MultiInterceptor> loader = ServiceLoader.load(MultiInterceptor.class);
        List<MultiInterceptor> interceptors = toInterceptorList(loader);
        MULTI_INTERCEPTORS = interceptors.toArray(new MultiInterceptor[0]);
    }

    public static void reloadCallbackDecorators() {
        if (DISABLE_CALLBACK_DECORATORS) {
            CALLBACK_DECORATORS = new CallbackDecorator[0];
        } else {
            ServiceLoader<CallbackDecorator> loader = ServiceLoader.load(CallbackDecorator.class);
            List<CallbackDecorator> interceptors = toInterceptorList(loader);
            CALLBACK_DECORATORS = interceptors.toArray(new CallbackDecorator[0]);
        }
    }

    private static <T extends MutinyInterceptor> List<T> toInterceptorList(ServiceLoader<T> loader) {
        List<T> interceptors = new ArrayList<>();
        for (T item : loader) {
            interceptors.add(item);
        }
        interceptors.sort(MutinyInterceptorComparator.INSTANCE);
        return interceptors;
    }

    public static void clearInterceptors() {
        UNI_INTERCEPTORS = new UniInterceptor[0];
        MULTI_INTERCEPTORS = new MultiInterceptor[0];
        CALLBACK_DECORATORS = new CallbackDecorator[0];
    }

    // For testing purpose only
    public static void resetDroppedExceptionHandler() {
        droppedExceptionHandler = PrintAndDumpThrowableConsumer.INSTANCE;
    }

    // For testing purpose only
    public static void resetCanCallerThreadBeBlockedSupplier() {
        canCallerThreadBeBlockedSupplier = AlwaysTrueBooleanSupplier.INSTANCE;
    }

    private Infrastructure() {
        // Avoid direct instantiation.
    }

    public static BooleanSupplier decorate(BooleanSupplier supplier) {
        BooleanSupplier current = supplier;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    public static <T> Predicate<T> decorate(Predicate<T> predicate) {
        Predicate<T> current = predicate;
        for (CallbackDecorator interceptor : callbackDecorators()) {
            current = interceptor.decorate(current);
        }
        return current;
    }

    /**
     * Log from an operator.
     * <p>
     * This method should never be called directly but only from {@link Multi#log(String)} and {@link Uni#log(String)}.
     *
     * @param identifier the event identifier
     * @param event the event as a string
     * @param value the value, if any or {@code null}
     * @param failure the failure, if any or {@code null}
     */
    public static void logFromOperator(String identifier, String event, Object value, Throwable failure) {
        operatorLogger.log(identifier, event, value, failure);
    }

    /**
     * Defines operator logging behavior for {@link Multi#log(String)} and {@link Uni#log(String)}.
     *
     * @param operatorLogger the new operator logger
     */
    public static void setOperatorLogger(OperatorLogger operatorLogger) {
        Infrastructure.operatorLogger = nonNull(operatorLogger, "operatorLogger");
    }

    // For testing purpose only
    public static void resetOperatorLogger() {
        Infrastructure.operatorLogger = PrintOperatorEventOperatorLogger.INSTANCE;
    }

    /**
     * Get the default overflow buffer size for {@link MultiOverflowStrategy#buffer()}.
     *
     * @return the default value
     */
    public static int getMultiOverflowDefaultBufferSize() {
        return multiOverflowDefaultBufferSize;
    }

    /**
     * Sets the default overflow buffer size for {@link MultiOverflowStrategy#buffer()}.
     *
     * @param size the buffer size, must be strictly positive
     */
    public static void setMultiOverflowDefaultBufferSize(int size) {
        multiOverflowDefaultBufferSize = positive(size, "size");
    }

    private static int resolveBufferSize(String property, int defaultValue, int minimum) {
        String propVal = System.getProperty(property);
        if (propVal != null) {
            return Math.max(minimum, Integer.parseInt(propVal));
        }
        return defaultValue;
    }

    /**
     * Get the xs buffer size (for internal usage).
     *
     * @return the buffer size
     */
    public static int getBufferSizeXs() {
        return bufferSizeXs;
    }

    /**
     * Set the xs buffer size (for internal usage).
     *
     * @param size the buffer size
     */
    public static void setBufferSizeXs(int size) {
        bufferSizeXs = positive(size, "size");
    }

    /**
     * Get the s buffer size (for internal usage).
     *
     * @return the buffer size
     */
    public static int getBufferSizeS() {
        return bufferSizeS;
    }

    /**
     * Set the s buffer size (for internal usage).
     *
     * @param size the buffer size
     */
    public static void setBufferSizeS(int size) {
        bufferSizeS = positive(size, "size");
    }

    /**
     * An operator logger for {@link Multi#log(String)} and {@link Uni#log(String)}.
     */
    @FunctionalInterface
    public interface OperatorLogger {

        /**
         * Actual logging behavior.
         *
         * @param identifier the event identifier
         * @param event the event as a string
         * @param value the value, if any or {@code null}
         * @param failure the failure, if any or {@code null}
         */
        void log(String identifier, String event, Object value, Throwable failure);
    }

    private static class MutinyInterceptorComparator implements Comparator<MutinyInterceptor> {

        private static final MutinyInterceptorComparator INSTANCE = new MutinyInterceptorComparator();

        @Override
        public int compare(MutinyInterceptor o1, MutinyInterceptor o2) {
            return Integer.compare(o1.ordinal(), o2.ordinal());
        }
    }

    private static class AlwaysTrueBooleanSupplier implements BooleanSupplier {

        private static final AlwaysTrueBooleanSupplier INSTANCE = new AlwaysTrueBooleanSupplier();

        @Override
        public boolean getAsBoolean() {
            return true;
        }
    }

    private static class PrintAndDumpThrowableConsumer implements Consumer<Throwable> {

        private static final PrintAndDumpThrowableConsumer INSTANCE = new PrintAndDumpThrowableConsumer();

        @Override
        public void accept(Throwable throwable) {
            System.err.println("[-- Mutiny had to drop the following exception --]");
            StackTraceElement element = Thread.currentThread().getStackTrace()[3];
            System.err.println("Exception received by: " + element.toString());
            throwable.printStackTrace();
            System.err.println("[------------------------------------------------]");
        }
    }

    private static class PrintOperatorEventOperatorLogger implements OperatorLogger {

        private static final PrintOperatorEventOperatorLogger INSTANCE = new PrintOperatorEventOperatorLogger();

        @Override
        public void log(String identifier, String event, Object value, Throwable failure) {
            String message = "[--> " + identifier + " | " + event;
            if (failure == null) {
                if (value != null) {
                    message = message + "(" + value + ")";
                } else {
                    message = message + "()";
                }
            } else {
                message = message + "(" + failure.getClass().getName() + "(\"" + failure.getMessage() + "\"))";
            }
            System.out.println(message);
        }
    }

}
