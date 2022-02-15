package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.builders.UnthrottledBroadcaster;

// TODO
public class MultiThrottledBroadcast {

    public <T> Multi<T> unthrottled(Multi<T> multi) {
        return new UnthrottledBroadcaster(multi);
    }
}
