package io.smallrye.mutiny.subscription;

import io.smallrye.mutiny.Context;

// TODO
public interface ContextSupport {

    // TODO: there is no need for caching the context in most operators, downwards calls to context() only happen once per withContext operator
    Context context();
}
