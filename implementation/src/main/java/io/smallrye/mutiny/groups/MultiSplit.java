package io.smallrye.mutiny.groups;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;

import java.util.function.Function;

import static io.smallrye.mutiny.helpers.ParameterValidation.nonNull;

@Experimental("TBD")
public class MultiSplit<T, K extends Enum<K>> {

    private final io.smallrye.mutiny.operators.multi.MultiSplitter<T, K> operator;

    public MultiSplit(Multi<T> upstream, Class<K> keyType, Function<T, K> splitter) {
        nonNull(upstream, "upstream");
        nonNull(keyType, "keyType");
        nonNull(splitter, "splitter");
        this.operator = new io.smallrye.mutiny.operators.multi.MultiSplitter<>(upstream, keyType, splitter);
    }


}
