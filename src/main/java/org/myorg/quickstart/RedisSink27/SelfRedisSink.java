package org.myorg.quickstart.RedisSink27;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class SelfRedisSink extends RichSinkFunction {


    private transient JedisPool pool;

    public void open(Configuration config) {
        pool = new JedisPool("localhost", 6379);
    }

    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(value.f0, value.f1);
        }
    }

    @Override
    public void close() throws Exception {
        pool.close();
    }

}
