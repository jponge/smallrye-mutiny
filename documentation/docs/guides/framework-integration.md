---
tags:
- guide
- advanced
---

# How can I integrate Mutiny with my framework?

Sometimes, Mutiny needs to execute tasks on other threads, such as monitoring time or delaying actions.
Most operators relying on such capacity let you pass either a `ScheduledExecutorService` or an `ExecutorService`.

## Default behavior

Mutiny defers executor creation and interceptor discovery to first use:

- The first call to `Infrastructure.getDefaultExecutor()` triggers a `ServiceLoader` lookup for `ExecutorConfiguration`. If no provider is found, Mutiny creates a cached thread pool and a `ScheduledExecutorService` that delegates to it.
- The first call that needs interceptors (e.g., creating a `Uni` or `Multi`) triggers `ServiceLoader` lookups for `UniInterceptor`, `MultiInterceptor`, and `CallbackDecorator`.

This lazy initialization means referencing Mutiny types does not force thread pool or classpath scanning costs at startup.

## Configuring the executor

You can replace the default executor with one managed by your platform using `Infrastructure.setDefaultExecutor()`:

```java linenums="1"
{{ insert('java/guides/infrastructure/InfrastructureTest.java', 'infra') }}
```

```java linenums="1"
{{ insert('java/guides/infrastructure/InfrastructureTest.java', 'set-infra') }}
```

Calling `setDefaultExecutor()` shuts down the previous executor.
Use `setDefaultExecutor(executor, false)` to skip shutdown if the previous executor is managed externally.

## Using `Infrastructure.configure()`

`Infrastructure.configure()` is a single-call alternative to multiple setter invocations.
It accepts an `InfrastructureConfiguration` object with fluent setters for:

- **`defaultExecutor`** — the executor for async operations (does _not_ shut down the previous one)
- **`uniInterceptors`**, **`multiInterceptors`**, **`callbackDecorators`** — interceptors applied during pipeline assembly
- **`droppedExceptionHandler`** — handler for exceptions that cannot be delivered to a subscriber
- **`canCallerThreadBeBlockedSupplier`** — controls blocking operator availability
- **`operatorLogger`** — logging backend for `Uni.log()` / `Multi.log()`
- **`completableFutureWrapper`** — wrapper applied to `CompletableFuture` conversions
- **`bufferSizeXs`**, **`bufferSizeS`**, **`multiOverflowDefaultBufferSize`** — internal buffer sizes

Only categories explicitly set in the configuration object are applied — unset categories are left unchanged.
When interceptor lists are provided (even empty ones), `ServiceLoader` discovery is bypassed for those categories.
This is useful for frameworks that know their interceptors at build time and want to avoid classpath scanning entirely.

```java linenums="1"
{{ insert('java/guides/infrastructure/InfrastructureTest.java', 'configure') }}
```

!!! tip
    
    If you are using Quarkus, the default executor is already configured to use the Quarkus worker thread pool.
    Logging is also configured correctly.
