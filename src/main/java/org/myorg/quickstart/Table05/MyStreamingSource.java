package org.myorg.quickstart.Table05;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyStreamingSource implements SourceFunction<Item> {

    private boolean isRunning = true;

    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext<Item> ctx) throws Exception {
        while(isRunning){
            Item item = generateItem();
            ctx.collect(item);

            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }

    //随机产生一条商品数据
    private Item generateItem(){
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}


class StreamingDemo {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStreamingSource()).map(new MapFunction<Item, Item>() {
            @Override
            public Item map(Item item) throws Exception {
                return item;
            }
        });

        // Flink 1.15 移除了 DataStream#split(OutputSelector)，这里用 filter 实现同样的分流
        DataStream<Item> evenSelect = source.filter(new FilterFunction<Item>() {
            @Override
            public boolean filter(Item value) throws Exception {
                return value.getId() % 2 == 0;
            }
        });

        DataStream<Item> oddSelect = source.filter(new FilterFunction<Item>() {
            @Override
            public boolean filter(Item value) throws Exception {
                return value.getId() % 2 != 0;
            }
        });


        // Flink 1.15 推荐用 Schema 描述字段（字符串形式的字段名重载已弃用）
        Schema itemSchema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .build();

        bsTableEnv.createTemporaryView("evenTable", evenSelect, itemSchema);
        bsTableEnv.createTemporaryView("oddTable", oddSelect, itemSchema);

        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");

        queryTable.printSchema();

        // toRetractStream/toDataStream 都已弃用，1.15 推荐用 toChangelogStream
        bsTableEnv.toChangelogStream(queryTable).print();

        bsEnv.execute("streaming sql job");
    }

}
