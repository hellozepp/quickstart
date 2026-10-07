package org.myorg.quickstart.SideOutPut10;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


/**
 * Flink 1.15 已经删除了 DataStream#split(OutputSelector) 和 SplitStream，
 * 官方推荐用两次 filter（或侧输出流，见 StreamingDemoSideOutPut）实现分流。
 */
class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));


        DataStreamSource<Tuple3<Integer,Integer,Integer>> items = env.fromCollection(data);

        DataStream<Tuple3<Integer, Integer, Integer>> zeroStream = items.filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
                return value.f0 == 0;
            }
        });

        DataStream<Tuple3<Integer, Integer, Integer>> oneStream = items.filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
                return value.f0 == 1;
            }
        });

        zeroStream.print();
        oneStream.printToErr();

        //打印结果
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }

}
