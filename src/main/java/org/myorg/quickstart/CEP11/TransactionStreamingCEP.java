package org.myorg.quickstart.CEP11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class TransactionStreamingCEP {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TransactionEvent> source = env.fromElements(
                new TransactionEvent("100XX", 0.0D, 1597905234000L),
                new TransactionEvent("100XX", 100.0D, 1597905235000L),
                new TransactionEvent("100XX", 200.0D, 1597905236000L),
                new TransactionEvent("100XX", 300.0D, 1597905237000L),
                new TransactionEvent("100XX", 400.0D, 1597905238000L),
                new TransactionEvent("100XX", 500.0D, 1597905239000L),
                new TransactionEvent("101XX", 0.0D, 1597905240000L),
                new TransactionEvent("101XX", 100.0D, 1597905241000L)


        ).assignTimestampsAndWatermarks(WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp())).keyBy(new KeySelector<TransactionEvent, Object>() {
            @Override
            public Object getKey(TransactionEvent value) throws Exception {
                return value.getAccout();
            }
        });

        Pattern<TransactionEvent, TransactionEvent> pattern = Pattern.<TransactionEvent>begin("start").where(
                new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent transactionEvent) {
                        return transactionEvent.getAmount() > 0;
                    }
                }
        ).timesOrMore(5)
         .within(Time.hours(24));

        PatternStream<TransactionEvent> patternStream = CEP.pattern(source, pattern);

        SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<TransactionEvent, AlertEvent>() {
            @Override
            public void processMatch(Map<String, List<TransactionEvent>> match, Context ctx, Collector<AlertEvent> out) throws Exception {

                List<TransactionEvent> start = match.get("start");
                List<TransactionEvent> next = match.get("next");
                System.err.println("start:" + start + ",next:" + next);

                out.collect(new AlertEvent(start.get(0).getAccout(), "连续有效交易！"));
            }
        });

        process.printToErr();
        env.execute("execute cep");

    }



}
