package com.shf.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * union的两个stream必须格式一致
 * 3> (foo,6)
 * 7> (bar,5)
 *
 * @author songhaifeng
 * @date 2022/8/23 22:59
 */
public class UnionJob {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(FOO, 1));

        List<Tuple2<String, Integer>> input2 = new ArrayList<>();
        input2.add(Tuple2.of(BAR, 1));
        input2.add(Tuple2.of(FOO, 1));
        input2.add(Tuple2.of(FOO, 1));
        input2.add(Tuple2.of(BAR, 1));
        input2.add(Tuple2.of(FOO, 1));
        input2.add(Tuple2.of(FOO, 1));

        env.fromCollection(input)
                .union(env.fromCollection(input2))
                .keyBy(event -> event.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
