package org.myorg.quickstart.RedisSink27;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

/**
 * 决定一条流数据怎么写进 Redis，配合 {@link RedisSinkV2} 使用。
 */
@FunctionalInterface
public interface RedisWriter<T> extends Serializable {

    void write(Jedis jedis, T value);
}
