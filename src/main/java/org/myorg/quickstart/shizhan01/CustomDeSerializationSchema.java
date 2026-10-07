package org.myorg.quickstart.shizhan01;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Flink 1.15 里旧的 org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
 * 已经随旧连接器一起弃用，新的接口是 KafkaRecordDeserializationSchema。
 */
public class CustomDeSerializationSchema implements KafkaRecordDeserializationSchema<ConsumerRecord<String, String>> {

    //这里返回一个ConsumerRecord<String,String>类型的数据，除了原数据还包括topic，offset，partition等信息
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ConsumerRecord<String, String>> out) {

        out.collect(new ConsumerRecord<String, String>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() == null ? null : new String(record.key()),
                record.value() == null ? null : new String(record.value())
        ));
    }

    //指定数据的输出类型
    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>(){});
    }
}
