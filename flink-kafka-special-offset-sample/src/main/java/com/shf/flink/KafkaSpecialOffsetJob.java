package com.shf.flink;

import com.shf.flink.event.UserBehaviorEvent;
import com.shf.flink.schema.UserBehaviorEventSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.shf.flink.constants.Constants.KAFKA_BOOTSTRAP_SERVER;
import static com.shf.flink.constants.Constants.SPECIAL_OFFSET_TOPIC;

/**
 * description :
 * 测试验证指定消费分区的起始和截止offset：
 * 通过日志打印，可观测其多次执行结果一致，累计消费了1500条message(消息预生成)
 * 6> (0,28,1660648880700,1660648880750)
 * 8> (2,29,1660648880700,1660648880750)
 * 6> (1,24,1660648880700,1660648880750)
 * 6> (1,176,1660648880750,1660648880800)
 * 6> (0,171,1660648880750,1660648880800)
 * 8> (2,254,1660648880750,1660648880800)
 * 6> (0,296,1660648880800,1660648880850)
 * 6> (1,313,1660648880800,1660648880850)
 * 8> (2,209,1660648880800,1660648880850)
 * <p>
 * 如官方说明： When all partitions have reached their stopping offsets, the source will exit.
 * https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/kafka/#boundedness
 *
 * @author songhaifeng
 * @date 2022/8/16 0:59
 */
public class KafkaSpecialOffsetJob {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpecialOffsetJob.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String topic = parameterTool.get("kafka-topic", SPECIAL_OFFSET_TOPIC);
        String brokers = parameterTool.get("brokers", KAFKA_BOOTSTRAP_SERVER);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        KafkaSource<UserBehaviorEvent> source = KafkaSource.<UserBehaviorEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setProperty("register.consumer.metrics", "false")
                .setProperties(properties)
                .setGroupId("sample-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
                //  设置起始offset
                .setStartingOffsets(new OffsetsInitializer() {
                    @Override
                    public Map<TopicPartition, Long> getPartitionOffsets(Collection<TopicPartition> partitions, PartitionOffsetsRetriever partitionOffsetsRetriever) {
                        Map<TopicPartition, Long> map = new HashMap<>();
                        LOG.info("setting startOffsets");
                        partitions.forEach(topicPartition -> {
                            LOG.info(topicPartition.topic() + " - " + topicPartition.partition());
                            map.put(topicPartition, 0L);
                        });
                        return map;
                    }

                    @Override
                    public OffsetResetStrategy getAutoOffsetResetStrategy() {
                        return OffsetResetStrategy.EARLIEST;
                    }
                })
                // 设置每个分区读取的目标截止offset
                .setBounded(new OffsetsInitializer() {
                    /**
                     *
                     * @param collection 获取所有分区
                     * @param partitionOffsetsRetriever partitionOffsetsRetriever可获取当前消费者每个分区的offset信息
                     * @return Map
                     */
                    @Override
                    public Map<TopicPartition, Long> getPartitionOffsets(Collection<TopicPartition> collection, PartitionOffsetsRetriever partitionOffsetsRetriever) {
                        Map<TopicPartition, Long> map = new HashMap<>();
                        LOG.info("setting endOffsets");
                        collection.forEach(topicPartition -> {
                            LOG.info(topicPartition.topic() + " - " + topicPartition.partition());
                            map.put(topicPartition, 500L);
                        });
                        return map;
                    }

                    /**
                     * The OffsetStrategy is only used when the offset initializer is used to initialize the
                     * starting offsets and the starting offsets is out of range.
                     * @return OffsetResetStrategy
                     */
                    @Override
                    public OffsetResetStrategy getAutoOffsetResetStrategy() {
                        return OffsetResetStrategy.EARLIEST;
                    }
                })
                .setDeserializer(new UserBehaviorEventSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);

        DataStreamSource<UserBehaviorEvent> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(2);

        // 每50ms统计期间内每个用户的行为次数，并打印输出
        streamSource.keyBy(UserBehaviorEvent::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(50)))
                .aggregate(new UserCountAggregateFunction(), new WindowResult())
                .print();

        env.execute("KafkaSpecialOffsetJob");
    }
}
