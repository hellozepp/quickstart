package org.myorg.quickstart.RedisSink27;


import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

public class RedisSink01 implements RedisWriter<Tuple2<String, String>> {

    /**
     * 使用 SET 命令写入：key = f0, value = f1
     */
    @Override
    public void write(Jedis jedis, Tuple2<String, String> data) {
        jedis.set(data.f0, data.f1);
    }
}
