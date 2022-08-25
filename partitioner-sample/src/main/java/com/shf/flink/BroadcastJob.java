package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description :
 * 打印示例如下：
 * 2022-08-25 17:27:38,755 [Map (4/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：70 ,value: 1
 * 2022-08-25 17:27:38,755 [Map (1/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：63 ,value: 1
 * 2022-08-25 17:27:38,755 [Map (2/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：68 ,value: 1
 * 2022-08-25 17:27:38,755 [Map (3/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：69 ,value: 1
 * 2022-08-25 17:27:38,756 [Map (1/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：63 ,value: 2
 * 2022-08-25 17:27:38,756 [Map (4/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：70 ,value: 2
 * 2022-08-25 17:27:38,756 [Map (3/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：69 ,value: 2
 * 2022-08-25 17:27:38,756 [Map (2/4)#0] INFO  com.shf.flink.BroadcastJob.map:31 - 当前线程id：68 ,value: 2
 *
 * @author songhaifeng
 * @date 2022/8/25 17:20
 */
@Slf4j
public class BroadcastJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //获取数据源
        DataStreamSource<Integer> text = env.fromElements(1, 3, 3, 4, 5, 6, 7, 8, 9);

        // 广播后将在每个并行分区上均输出对应的item元素
        DataStream<Integer> num = text.broadcast().map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                log.info("当前线程id：{} ,value: {} ", Thread.currentThread().getId(), value);
                return value;
            }
        });
        // 每个分区均有9个item，windowAll后再一个task进行sum操作，累计36个item，计算18组数据
        num.countWindowAll(2).sum(0).print().setParallelism(1);
        env.execute();
    }

}
