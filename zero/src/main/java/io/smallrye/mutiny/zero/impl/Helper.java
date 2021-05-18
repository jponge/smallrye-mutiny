package io.smallrye.mutiny.zero.impl;

import java.util.concurrent.atomic.AtomicLong;

class Helper {

    public static IllegalArgumentException negativeRequest(long n) {
        return new IllegalArgumentException("Requested " + n + " items (must be > 0L)");
    }

    public static long add(AtomicLong requested, long requests) {
        while (true) {
            long expected = requested.get();
            if (expected == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long update = expected + requests;
            if (update < 0L) {
                update = Long.MAX_VALUE;
            }
            if (requested.compareAndSet(expected, update)) {
                return expected;
            }
        }
    }
}
