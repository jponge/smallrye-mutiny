package io.smallrye.mutiny.nativetests;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class YoloTest {

    @Test
    void yolo() {
        System.out.println("Yolo!");
        assertThat("abc").isEqualTo("abc");
    }
}
