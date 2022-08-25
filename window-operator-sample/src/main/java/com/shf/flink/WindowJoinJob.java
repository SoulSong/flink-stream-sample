package com.shf.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * Window join:将两个流中有相同 key 和处在相同 window 里的元素去做 Join(笛卡尔积)。它的执行的逻辑比较像 Inner Join，必须同时满足 Join key 相同，而且在同一个 Window 里元素才能够在最终结果中输出。其底层使用的是coGroup算子：
 * result->:7> (bar,2,12)
 * result->:7> (bar,2,15)
 * result->:3> (foo,1,13)
 * result->:3> (foo,1,14)
 * result->:3> (foo,3,13)
 * result->:3> (foo,3,14)
 *
 * @author songhaifeng
 * @date 2022/8/23 23:14
 */
public class WindowJoinJob {

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
                .join(env.fromCollection(input2))
                // 通过keySelect提取等于关联字段值
                .where(event -> event.f0)
                .equalTo(event2 -> event2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(20)))
                // 提取两个stream的数据进行拉宽等操作
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    // 其类似于sql中的innerJoin，若在当前窗口内没有被两个stream关联到的元素不会被发出，不会被应用
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    }
                })
                .print("result->").setParallelism(4);

        env.execute();
    }
}
