package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * 根据指定的 key 和窗口将两个数据流组合在一起。与windowJoin的差异是，在apply中获取到的是两个stream中相同key的集合
 * first : [(foo,1), (foo,3)] ; second : [(foo,13), (foo,14)]
 * first : [(bar,2)] ; second : [(bar,12), (bar,15)]
 *
 * result->> (foo,1,13)
 * result->> (foo,1,14)
 * result->> (foo,3,13)
 * result->> (foo,3,14)
 * result->> (bar,2,12)
 * result->> (bar,2,15)
 *
 * @author songhaifeng
 * @date 2022/8/24 1:21
 */
@Slf4j
public class CoGroupJob {

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
                // 与windowJoin的差异是，在apply中获取到的是两个stream中相同key的集合
                .coGroup(env.fromCollection(input2))
                // 通过keySelect提取等于关联字段值
                .where(event -> event.f0)
                .equalTo(event2 -> event2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(20)))
                // 提取两个stream的数据进行拉宽等操作，需要手动对集合遍历
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        log.info("first : {} ; second : {}", first, second);
                        first.forEach(item -> {
                            second.forEach(item2 -> {
                                out.collect(Tuple3.of(item.f0, item.f1, item2.f1));
                            });
                        });
                    }
                })
                .print("result->").setParallelism(1);

        env.execute();
    }

}
