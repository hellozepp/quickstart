package org.myorg.quickstart.watermark08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowWaterMark {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);

        // 事件时间语义由 WatermarkStrategy 决定，不需要再调用 setStreamTimeCharacteristic
        // forBoundedOutOfOrderness(5秒) 等价于旧写法的 maxOutOfOrderness = 5000L
        DataStream<String> dataStream = env
                .socketTextStream("127.0.0.1", 9000)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) ->
                                        Long.parseLong(element.split(",")[1])));

        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {

                String[] split = s.split(",");
                return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));
            }
        })
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .minBy(1)
                .print();

        env.execute("WaterMark Test Demo");

    }//

}
