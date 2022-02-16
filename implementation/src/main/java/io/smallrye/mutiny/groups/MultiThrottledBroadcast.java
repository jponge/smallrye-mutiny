package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.builders.broadcasters.UnthrottledBroadcaster;

// TODO
public class MultiThrottledBroadcast {

    public <T> Multi<T> unthrottled(Multi<T> multi) {
        return new UnthrottledBroadcaster<>(multi, BroadcasterConf.create());
    }

    public <T> Multi<T> unthrottled(Multi<T> multi, UnthrottledBroadcasterConf configuration) {
        return new UnthrottledBroadcaster<>(multi, configuration);
    }
}
