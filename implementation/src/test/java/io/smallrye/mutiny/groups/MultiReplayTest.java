package io.smallrye.mutiny.groups;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import static org.junit.jupiter.api.Assertions.*;

class MultiReplayTest {

    @Test
    void playground() {
        Multi<Integer> range = Multi.createFrom().range(1, 300);
        Multi<Integer> multi = Multi.createBy().replaying().multi(range);

        AssertSubscriber<Integer> a = AssertSubscriber.create();
        multi.subscribe(a);

        AssertSubscriber<Integer> b = AssertSubscriber.create();
        multi.subscribe(b);

        System.out.println("== 1 ==");

        a.request(3);
        System.out.println("a = " + a.getItems());
        a.request(1);
        System.out.println("a = " + a.getItems());

        System.out.println("== 2 ==");

        b.request(2);
        System.out.println("b = " + b.getItems());
        b.request(300);
        a.request(50);
        System.out.println("a = " + a.getItems());
        System.out.println("b = " + b.getItems());

        System.out.println("== 3 ==");

        b.request(200);
        a.request(10);
        System.out.println("a = " + a.getItems());
        System.out.println("b = " + b.getItems());

        AssertSubscriber<Integer> c = AssertSubscriber.create(Long.MAX_VALUE);
        multi.subscribe(c);

        System.out.println("== 4 ==");

        System.out.println("a = " + a.getItems());
        System.out.println("b = " + b.getItems());
        System.out.println("c = " + c.getItems());
    }
}