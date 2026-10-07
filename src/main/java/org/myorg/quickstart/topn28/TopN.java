package org.myorg.quickstart.topn28;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.myorg.quickstart.RedisSink27.RedisSinkV2;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.RedisSink27.RedisSink02;

import java.time.Duration;
import java.util.*;

public class TopN {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        // Flink 1.15 的新数据源 API：KafkaSource（FlinkKafkaConsumer 已弃用）
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test")
                .setGroupId("topn-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        DataStream<OrderDetail> orderStream = stream.map(message -> JSON.parseObject(message, OrderDetail.class));

        // 事件时间 + 水印：Flink 1.12 之后统一用 WatermarkStrategy（旧的 AssignerWithPeriodicWatermarks 已弃用）
        DataStream<OrderDetail> dataStream = orderStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));

        DataStream<OrderDetail> reduce = dataStream
                .keyBy((KeySelector<OrderDetail, Object>) value -> value.getUserId())
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(20)))
                .reduce(new ReduceFunction<OrderDetail>() {
                    @Override
                    public OrderDetail reduce(OrderDetail value1, OrderDetail value2) throws Exception {
                        return new OrderDetail(
                                value1.getUserId(), value1.getItemId(), value1.getCiteName(), value1.getPrice() + value2.getPrice(), value1.getTimeStamp()
                        );
                    }
                });


        //每20秒计算一次
        DataStream<Tuple2<Double, OrderDetail>> process = reduce.windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(new ProcessAllWindowFunction<OrderDetail, Tuple2<Double, OrderDetail>, TimeWindow>() {
                             @Override
                             public void process(Context context, Iterable<OrderDetail> elements, Collector<Tuple2<Double, OrderDetail>> out) throws Exception {
                                 TreeMap<Double, OrderDetail> treeMap = new TreeMap<Double, OrderDetail>(new Comparator<Double>() {
                                     @Override
                                     public int compare(Double x, Double y) {
                                         return (x < y) ? -1 : 1;
                                     }
                                 });

                                 Iterator<OrderDetail> iterator = elements.iterator();
                                 if (iterator.hasNext()) {
                                     treeMap.put(iterator.next().getPrice(), iterator.next());
                                     if (treeMap.size() > 10) {
                                         treeMap.pollLastEntry();
                                     }
                                 }

                                 for (Map.Entry<Double, OrderDetail> entry : treeMap.entrySet()) {
                                     out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                                 }
                             }
                         }
                );
        process.sinkTo(new RedisSinkV2<>("localhost", 6379,
                (jedis, data) -> jedis.hset("TOPN:", String.valueOf(data.f0), data.f1.toString())));

        env.execute("execute topn");


    }
}//
