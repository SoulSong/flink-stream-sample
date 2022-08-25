package com.shf.flink;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * 构造原始数据集
 * input.add(Tuple2.of(FOO, now));
 * input.add(Tuple2.of(BAR, now + 1));
 *
 * input.add(Tuple2.of(BAR, now + 2));
 * input.add(Tuple2.of(BAR, now + 3));
 * input.add(Tuple2.of(FOO, now + 4));
 * input.add(Tuple2.of(BAR, now + 5));
 * input.add(Tuple2.of(FOO, now + 6));
 *
 * input.add(Tuple2.of(FOO, now + 7));
 * input.add(Tuple2.of(BAR, now + 8));
 * input.add(Tuple2.of(FOO, now + 9));
 * input.add(Tuple2.of(FOO, now + 10));
 *
 * 基于eventTime，滚动窗口5ms，触发器采用2ms。其聚合打印如下：
 * 3> (foo,[1661247865098],TimeWindow{start=1661247865095, end=1661247865100})
 * 7> (bar,[1661247865099],TimeWindow{start=1661247865095, end=1661247865100})
 * <p>
 * 3> (foo,[1661247865102, 1661247865104],TimeWindow{start=1661247865100, end=1661247865105})
 * 7> (bar,[1661247865100, 1661247865101, 1661247865103],TimeWindow{start=1661247865100, end=1661247865105})
 * 7> (bar,[1661247865100, 1661247865101, 1661247865103],TimeWindow{start=1661247865100, end=1661247865105})
 * <p>
 * 3> (foo,[1661247865105, 1661247865107, 1661247865108],TimeWindow{start=1661247865105, end=1661247865110})
 * 3> (foo,[1661247865105, 1661247865107, 1661247865108],TimeWindow{start=1661247865105, end=1661247865110})
 * 7> (bar,[1661247865106],TimeWindow{start=1661247865105, end=1661247865110})
 * 7> (bar,[1661247865106],TimeWindow{start=1661247865105, end=1661247865110})
 * 3> (foo,[1661247865105, 1661247865107, 1661247865108],TimeWindow{start=1661247865105, end=1661247865110})
 *
 * 可观测到，同一个window被多次触发计算并发送至processWindowFunction，processWindowFunction接收到的聚合结果是累计值，输出到目标存储是需要进行update操作。
 *
 * @author songhaifeng
 * @date 2022/8/23 15:22
 */
@Slf4j
public class EventTimeTriggerJob {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().setAutoWatermarkInterval(1);

        DataStream<Tuple2<String, Long>> dataStream = env.addSource(new DataSource());

        // 设置watermark，并提取属性值作为eventTime
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarksStream = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                // 由于生产的数据timestamp无乱序，可直接使用forMonotonousTimestamps，无需设置延迟
                .<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.f1));

        watermarksStream.keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .trigger(ContinuousEventTimeTrigger.of(Time.milliseconds(2)))
                // 将同一个window的item项时间戳combine后输出
                .aggregate(new AggregateFunction<Tuple2<String, Long>, ListAccumulator<Long>, List<Long>>() {
                    @Override
                    public ListAccumulator<Long> createAccumulator() {
                        return new ListAccumulator<>();
                    }

                    @Override
                    public ListAccumulator<Long> add(Tuple2<String, Long> value, ListAccumulator<Long> accumulator) {
                        accumulator.add(value.f1);
                        return accumulator;
                    }

                    @Override
                    public List<Long> getResult(ListAccumulator<Long> accumulator) {
                        return accumulator.getLocalValue();
                    }

                    @Override
                    public ListAccumulator<Long> merge(ListAccumulator<Long> a, ListAccumulator<Long> b) {
                        a.merge(b);
                        return a;
                    }
                }, new ProcessWindowFunction<List<Long>, Tuple3<String, List<Long>, TimeWindow>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<List<Long>> elements, Collector<Tuple3<String, List<Long>, TimeWindow>> out) throws Exception {
                        log.info("key -> {} ; elements -> {} ; window -> {}", key, elements.iterator().next(), context.window());
                        out.collect(Tuple3.of(key, Lists.newArrayList(elements.iterator().next()), context.window()));
                    }
                })
                .print();

        env.execute();
    }

    /**
     * 如下示例是一个单并行度的source，如果需要多并行度source，需要实现{@link ParallelSourceFunction}接口
     */
    private static class DataSource implements SourceFunction<Tuple2<String, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            final long now = System.currentTimeMillis();
            log.info("now : {}", now);
            List<Tuple2<String, Long>> input = new ArrayList<>();
            input.add(Tuple2.of(FOO, now));
            input.add(Tuple2.of(BAR, now + 1));
            input.add(Tuple2.of(BAR, now + 2));
            input.add(Tuple2.of(BAR, now + 3));
            input.add(Tuple2.of(FOO, now + 4));
            input.add(Tuple2.of(BAR, now + 5));
            input.add(Tuple2.of(FOO, now + 6));
            input.add(Tuple2.of(FOO, now + 7));
            input.add(Tuple2.of(BAR, now + 8));
            input.add(Tuple2.of(FOO, now + 9));
            input.add(Tuple2.of(FOO, now + 10));
            int i = 0;
            while (running && i < input.size()) {
                ctx.collect(input.get(i));
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
