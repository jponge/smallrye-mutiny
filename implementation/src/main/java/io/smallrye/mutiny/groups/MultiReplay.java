package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.operators.multi.builders.ReplayingMulti;

public class MultiReplay {

    public <T> Multi<T> multi(Multi<T> multi) {
        return new ReplayingMulti<>(ParameterValidation.nonNull(multi, "multi"));
    }
}
