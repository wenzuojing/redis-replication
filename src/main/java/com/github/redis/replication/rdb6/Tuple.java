package com.github.redis.replication.rdb6;

/**
 * Created by wens on 15-12-22.
 */
public class Tuple<A, B> {

    private A a;

    private B b;

    public Tuple(A a, B b) {
        this.a = a;
        this.b = b;
    }

    public A getA() {
        return a;
    }

    public void setA(A a) {
        this.a = a;
    }

    public B getB() {
        return b;
    }

    public void setB(B b) {
        this.b = b;
    }


    public static <A, B> Tuple of(A a, B b) {
        return new Tuple(a, b);
    }


}
