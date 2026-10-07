package org.myorg.quickstart.RedisSink27;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;

public class RedisConnector {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> stream = env.fromElements("Flink","Spark","Storm").map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s, s+"_sink2");
            }
        });

        stream.sinkTo(new RedisSinkV2<>("localhost", 6379, new RedisSink02()));
        env.execute("redis sink01");
    }
}
