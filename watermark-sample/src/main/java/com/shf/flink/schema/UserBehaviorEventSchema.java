package com.shf.flink.schema;

import com.alibaba.fastjson.JSON;
import com.shf.flink.event.UserBehaviorEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class UserBehaviorEventSchema implements KafkaRecordDeserializationSchema<UserBehaviorEvent> {
    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public TypeInformation<UserBehaviorEvent> getProducedType() {
        return TypeInformation.of(UserBehaviorEvent.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<UserBehaviorEvent> collector) throws IOException {
        collector.collect(JSON.parseObject(consumerRecord.value(), UserBehaviorEvent.class));
    }
}
