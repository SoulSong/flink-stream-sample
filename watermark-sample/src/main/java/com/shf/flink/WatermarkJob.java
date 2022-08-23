package com.shf.flink;

import com.shf.flink.event.UserBehaviorEvent;
import com.shf.flink.schema.UserBehaviorEventSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

import static com.shf.flink.producer.Constants.KAFKA_BOOTSTRAP_SERVER;
import static com.shf.flink.producer.Constants.TOPIC;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2022/8/17 13:45
 */
@Slf4j
public class WatermarkJob {

    public static void main(String[] args) throws Exception {
        KafkaSource<UserBehaviorEvent> source = KafkaSource.<UserBehaviorEvent>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
                .setTopics(TOPIC)
                .setProperty("register.consumer.metrics", "false")
                .setGroupId("sample-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new UserBehaviorEventSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<UserBehaviorEvent> streamSource = env.fromSource(source,
                // Watermark解决数据延迟和乱序
                WatermarkStrategy.<UserBehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> {
                            // 对于kafka数据源，recordTimestamp即为consumerRecord对应的timestamp，如下
                            // Offset: 79   Key: empty   Timestamp: 2022-08-17 23:36:29.978 Headers: empty
                            // eventTS [1660750589948] - recordTimestamp [1660750589978]
                            log.info("eventTS [{}] - recordTimestamp [{}]", element.getTs(), recordTimestamp);
                            return element.getTs();
                        })
                        // 处理闲置分区，事件间隙
                        .withIdleness(Duration.ofSeconds(20)),
                "Kafka Source").setParallelism(1);

        // 收集迟到数据
        OutputTag<UserBehaviorEvent> lateOutputUserBehavior = new OutputTag<UserBehaviorEvent>("late-userBehavior-data") {
        };

        SingleOutputStreamOperator outputStream = streamSource.keyBy(UserBehaviorEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                // Setting an allowed lateness is only valid for event-time windows. By default, the allowed lateness is {@code 0L}.
                // 解决数据丢失，允许迟到5s，在迟到数据在5S内到来时，其每个元素均会触发窗口的重新计算，需要在sink输出时考虑update
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateOutputUserBehavior)
                // 触发条件为：WaterMark 超过 endTime of window
                .process(new ProcessWindowFunction<UserBehaviorEvent, Tuple5<Integer, Integer, Long, Long, Long>, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<UserBehaviorEvent> elements, Collector<Tuple5<Integer, Integer, Long, Long, Long>> out) throws Exception {
                        long currentWatermark = context.currentWatermark();
                        long processingTime = context.currentProcessingTime();
                        TimeWindow timeWindow = context.window();
                        log.info("userId [{}] - currentWatermark [{}] - processingTime [{}] - startTime [{}] - endTime [{}]", integer, currentWatermark, processingTime, timeWindow.getStart(), timeWindow.getEnd());
                        // 对window内的数据进行排序，保证数据的顺序
                        List<UserBehaviorEvent> list = Lists.newArrayList(elements);
                        for (UserBehaviorEvent userBehaviorEvent : list) {
                            log.info(userBehaviorEvent.toString());
                        }
                        out.collect(Tuple5.of(integer, list.size(), currentWatermark, timeWindow.getStart(), timeWindow.getEnd()));
                    }
                });

        outputStream.print();

        outputStream.getSideOutput(lateOutputUserBehavior).print("late>> ");
        env.execute("WatermarkJob");
    }

}
