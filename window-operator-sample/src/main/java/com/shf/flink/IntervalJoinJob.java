package com.shf.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * 需要特别注意IntervalJoin仅适用于两个keyedStream。
 * 根据 key 相等并且满足指定的时间范围内（e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound）的条件（一个流必须落在另一个流的时间戳的一定时间范围内）将分别属于两个 keyed stream 的元素 e1 和 e2 Join 在一起。
 * <p>
 * 加入了时间窗口的限定，就使得我们可以对超出时间范围的数据做一个清理，这样的话就不需要去保留全量的 State。
 * <p>
 * Interval Join 是同时支持 processing time 和 even time去定义时间的。如果使用的是 processing time，Flink 内部会使用系统时间去划分窗口，并且去做相关的 state 清理。如果使用 even time 就会利用 Watermark 的机制去划分窗口，并且做 State 清理。
 *
 * @author songhaifeng
 * @date 2022/8/23 23:14
 */
public class IntervalJoinJob {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(BAR, 2));
        input.add(Tuple2.of(FOO, 3));

        List<Tuple2<String, Integer>> input2 = new ArrayList<>();
        input2.add(Tuple2.of(BAR, 12));
        input2.add(Tuple2.of(FOO, 13));
        input2.add(Tuple2.of(FOO, 14));
        input2.add(Tuple2.of(BAR, 15));

        env.fromCollection(input)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> System.currentTimeMillis())))
                .keyBy(event -> event.f0)
                .intervalJoin(env.fromCollection(input2)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner(((element, recordTimestamp) -> System.currentTimeMillis())))
                        .keyBy(event2 -> event2.f0))
                // 指定使用eventTime，其根据上面的Watermark进行窗口处理
                .inEventTime()
                // lower and upper bound
                // lower为2 upper为2场景下的打印
                // result->> (bar,2,15)
                // result->> (foo,3,13)
                // result->> (foo,3,14)
//                .between(Time.milliseconds(-2), Time.milliseconds(2))
                // lower为20 upper为2场景下的打印
                // result->> (bar,2,12)
                // result->> (bar,2,15)
                // result->> (foo,3,13)
                // result->> (foo,3,14)
                .between(Time.milliseconds(-20), Time.milliseconds(2))
                // 默认情况下，上下界也被包括在区间内，但 .lowerBoundExclusive() 和 .upperBoundExclusive() 可以将它们排除在外。
                .upperBoundExclusive()
                .lowerBoundExclusive()
                // 提取两个stream的数据进行拉宽等操作，keyedStream使用precess，window使用apply
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        out.collect(Tuple3.of(left.f0, left.f1, right.f1));
                    }
                })
                .print("result->").setParallelism(1);

        env.execute();
    }
}
