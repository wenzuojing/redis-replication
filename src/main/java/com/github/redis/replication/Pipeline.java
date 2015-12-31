package com.github.redis.replication;

/**
 * Created by wens on 15-12-31.
 */
public interface Pipeline {

    /**
     * SET key "value"
     * @param cmd
     */
    void process(String[] cmd );
}
