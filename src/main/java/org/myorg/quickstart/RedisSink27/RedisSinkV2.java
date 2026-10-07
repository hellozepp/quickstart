package org.myorg.quickstart.RedisSink27;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

/**
 * 基于 Flink 1.15 新版 Sink API（FLIP-143 Sink V2）+ Jedis 实现的 Redis Sink。
 *
 * 说明：Flink 官方没有再维护 Redis 连接器（老的 flink-connector-redis 还是 Flink 1.1.5 时代的产物，
 * 且内部用的是已经过时的 SinkFunction 体系），所以这里直接用新 Sink API 自己实现。
 */
public class RedisSinkV2<T> implements Sink<T> {

    private final String host;
    private final int port;
    private final RedisWriter<T> writer;

    public RedisSinkV2(String host, int port, RedisWriter<T> writer) {
        this.host = host;
        this.port = port;
        this.writer = writer;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new RedisSinkWriter<>(host, port, writer);
    }

    private static class RedisSinkWriter<T> implements SinkWriter<T> {

        private final String host;
        private final int port;
        private final RedisWriter<T> writer;

        private transient JedisPool pool;

        RedisSinkWriter(String host, int port, RedisWriter<T> writer) {
            this.host = host;
            this.port = port;
            this.writer = writer;
        }

        @Override
        public void write(T element, Context context) throws IOException {
            if (pool == null) {
                pool = new JedisPool(host, port);
            }
            try (Jedis jedis = pool.getResource()) {
                writer.write(jedis, element);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // 每条记录写完即生效，无需额外 flush
        }

        @Override
        public void close() {
            if (pool != null) {
                pool.close();
            }
        }
    }
}
