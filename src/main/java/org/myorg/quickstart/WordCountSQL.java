package org.myorg.quickstart;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

/**
 * Flink 1.15 的 Table API 已经统一成 TableEnvironment：
 * 老的 org.apache.flink.table.api.java.BatchTableEnvironment 已经删除。
 */
public class WordCountSQL {

    public static void main(String[] args) {

        // 1. 批模式 TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2. 构造一张 word,frequency 的表
        Table wordCount = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("word", DataTypes.STRING()),
                        DataTypes.FIELD("frequency", DataTypes.BIGINT())),
                Row.of("hello", 1L),
                Row.of("flink", 1L),
                Row.of("hello", 1L),
                Row.of("lagou", 1L));

        wordCount.printSchema();

        // 3. 注册成临时视图，然后用 SQL 查询
        tableEnv.createTemporaryView("WordCount", wordCount);

        Table result = tableEnv.sqlQuery(
                "select word as word, sum(frequency) as frequency from WordCount group by word");

        // 4. 打印结果
        result.execute().print();
    }
}
