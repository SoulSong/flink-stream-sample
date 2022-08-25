package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description :
 * 按照奇偶数进行分区
 *
 * @author songhaifeng
 * @date 2022/8/25 16:42
 */
@Slf4j
public class CustomPartitionerJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        DataStreamSource<Integer> text = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        //对数据进行转换，把Integer类型转成tuple1类型
        DataStream<Tuple1<Integer>> tupleData = text.map((MapFunction<Integer, Tuple1<Integer>>) Tuple1::new, TypeInformation.of(new TypeHint<Tuple1<Integer>>() {
        }));

        //分区之后的数据
        DataStream<Tuple1<Integer>> partitionData = tupleData.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                if (key % 2 == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }, (KeySelector<Tuple1<Integer>, Integer>) value -> value.f0);

        DataStream<Integer> result = partitionData.map(new MapFunction<Tuple1<Integer>, Integer>() {
            @Override
            public Integer map(Tuple1<Integer> value) throws Exception {
                log.info("当前线程id：{} ,value: {} ", Thread.currentThread().getId(), value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        env.execute("SteamingDemoWithMyParitition");
    }

}
