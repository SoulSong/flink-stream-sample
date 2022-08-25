package com.shf.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * 假设双流join时，一个表为维度表，需要从外部加载器全部数据后与当前窗口数据流进行join操作，可借助{@link RichFlatMapFunction}实现外部数据加载：
 * result->> (bar,2,12)
 * result->> (foo,1,11)
 * result->> (foo,1,13)
 * result->> (foo,3,11)
 * result->> (foo,3,13)
 *
 * @author songhaifeng
 * @date 2022/8/23 23:14
 */
public class DimensionJoinJob {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(BAR, 2));
        input.add(Tuple2.of(FOO, 3));

        env.fromCollection(input)
                .join(env.fromElements(1).flatMap(new RichFlatMapFunction<Integer, Tuple2<String, Integer>>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化数据源读取连接池等
                    }

                    @Override
                    public void flatMap(Integer value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("load extend data");
                        out.collect(Tuple2.of(FOO, 11));
                        out.collect(Tuple2.of(BAR, 12));
                        out.collect(Tuple2.of(FOO, 13));
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        // 关闭释放数据源连接池等
                    }
                }))
                // 通过keySelect提取等于关联字段值
                .where(event -> event.f0)
                .equalTo(event2 -> event2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                // 提取两个stream的数据进行拉宽等操作
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    }
                })
                .print("result->")
                // 设置sink的并行度为1，方便观察
                .setParallelism(1);

        env.execute();
    }
}
