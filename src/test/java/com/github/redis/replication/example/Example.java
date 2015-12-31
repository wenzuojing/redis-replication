package com.github.redis.replication.example;

import com.github.redis.replication.Pipeline;
import com.github.redis.replication.Replication;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by wens on 15-12-31.
 */
public class Example {

    public static void main(String[] args) throws IOException {



        Replication replication = new Replication("172.16.1.152", 6379, new Pipeline() {

            @Override
            public void process(String[] cmd) {
                System.out.println(Arrays.toString(cmd));
            }
        });

        replication.doReplication();

    }
}
