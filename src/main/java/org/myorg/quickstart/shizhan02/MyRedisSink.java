package org.myorg.quickstart.shizhan02;


import org.apache.flink.api.java.tuple.Tuple3;
import org.myorg.quickstart.RedisSink27.RedisWriter;
import redis.clients.jedis.Jedis;

public class MyRedisSink implements RedisWriter<Tuple3<String,String, Integer>>{

    private static final String HASH_KEY = "flink_pv_uv";

    /**
     * 使用 HSET 命令写入：hash = flink_pv_uv, field = f1, value = f2
     */
    @Override
    public void write(Jedis jedis, Tuple3<String, String, Integer> data) {
        jedis.hset(HASH_KEY, data.f1, data.f2.toString());
    }
}
