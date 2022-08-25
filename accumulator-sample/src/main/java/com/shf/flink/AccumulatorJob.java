package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * description :
 * 单个作业的所有累加器共享一个命名空间。因此你可以在不同的操作 function 里面使用同一个累加器。Flink 会在内部将所有具有相同名称的累加器合并起来。
 *
 * @author songhaifeng
 * @date 2022/8/25 17:20
 */
@Slf4j
public class AccumulatorJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //获取数据源
        DataStreamSource<Integer> text = env.fromElements(1, 3, 3, 4, 5, 6, 7, 8, 9);

        // 广播后将在每个并行分区上均输出对应的item元素
        DataStream<Integer> num = text.broadcast().process(new ProcessFunction<Integer, Integer>() {
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                this.numLines.add(1);
            }
        });
        // 根据累加器的名称获取其结果
        // 2022-08-25 18:52:37,472 [main] INFO  com.shf.flink.AccumulatorJob.main:47 - num-lines : 36
        log.info("num-lines : {}", env.execute().getAccumulatorResult("num-lines").toString());
    }

}
