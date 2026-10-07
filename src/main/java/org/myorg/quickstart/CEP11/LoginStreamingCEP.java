package org.myorg.quickstart.CEP11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LoginStreamingCEP {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<LogInEvent> source = env.fromElements(
                new LogInEvent(1L, "fail", 1597905234000L),
                new LogInEvent(1L, "success", 1597905235000L),
                new LogInEvent(2L, "fail", 1597905236000L),
                new LogInEvent(2L, "fail", 1597905237000L),
                new LogInEvent(2L, "fail", 1597905238000L),
                new LogInEvent(3L, "fail", 1597905239000L),
                new LogInEvent(3L, "success", 1597905240000L)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<LogInEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp())).keyBy(new KeySelector<LogInEvent, Object>() {
            @Override
            public Object getKey(LogInEvent value) throws Exception {
                return value.getUserId();
            }
        });

        Pattern<LogInEvent, LogInEvent> pattern = Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
                return value.getIsSuccess().equals("fail");
            }
        }).next("next").where(new IterativeCondition<LogInEvent>() {
            @Override
            public boolean filter(LogInEvent value, Context<LogInEvent> ctx) throws Exception {
                return value.getIsSuccess().equals("fail");
            }
        }).within(Time.seconds(5));

        PatternStream<LogInEvent> patternStream = CEP.pattern(source, pattern);

        SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<LogInEvent, AlertEvent>() {
            @Override
            public void processMatch(Map<String, List<LogInEvent>> match, Context ctx, Collector<AlertEvent> out) throws Exception {

                List<LogInEvent> start = match.get("start");
                List<LogInEvent> next = match.get("next");
                System.err.println("start:" + start + ",next:" + next);


                out.collect(new AlertEvent(String.valueOf(start.get(0).getUserId()), "出现连续登陆失败"));
            }
        });

        process.printToErr();
        env.execute("execute cep");

    }



}
