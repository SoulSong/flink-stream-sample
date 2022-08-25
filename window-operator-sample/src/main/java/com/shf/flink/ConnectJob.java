package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * 其类似于union，但相对union更加灵活，不需要两个stream完全一致，且合并后的stream类型可重定义。同时支持借助state算子共享两个steam的状态，可借助TimeState进行逻辑处理。
 * <p>
 * result->> (bar,2,input)
 * result->> (bar,27,input2)
 * result->> (foo,1,input)
 * result->> (foo,3,input)
 * result->> (foo,27,input2)
 *
 * @author songhaifeng
 * @date 2022/8/23 23:14
 */
@Slf4j
public class ConnectJob {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(FOO, 3));
        input.add(Tuple2.of(BAR, 2));

        List<Tuple2<String, Integer>> input2 = new ArrayList<>();
        input2.add(Tuple2.of(BAR, 12));
        input2.add(Tuple2.of(FOO, 13));
        input2.add(Tuple2.of(FOO, 14));
        input2.add(Tuple2.of(BAR, 15));

        // 将stream1进行keyBy/window等操作后与stream2进行connect操作，stream2也优先进行keyBy/window等操作。最后将两个stream数据量合并，生成新的格式
        // 两个stream可以有各自的window/trigger等规则
        env.fromCollection(input)
                .keyBy(e -> e.f0)
                .countWindow(1)
                .apply(new RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, GlobalWindow>() {
                    @Override
                    public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> item : input) {
                            sum += item.f1;
                        }
                        log.info("input : {}", input);
                        out.collect(Tuple2.of(s, sum));
                    }
                }).keyBy(e -> e.f0)
                .connect(env.fromCollection(input2).keyBy(e -> e.f0)
                        .countWindow(2)
                        .apply(new RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, GlobalWindow>() {
                            @Override
                            public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                                int sum = 0;
                                for (Tuple2<String, Integer> item : input) {
                                    sum += item.f1;
                                }
                                log.info("input2 : {}", input);
                                out.collect(Tuple2.of(s, sum));
                            }
                        }).keyBy(e -> e.f0))
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, String>>() {

                    // 流1的状态
                    ValueState<Tuple2<String, Integer>> state1;
                    // 流2的状态
                    ValueState<Tuple2<String, Integer>> state2;

                    // 定义一个用于删除定时器的状态
                    ValueState<Long> timeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        state1 = getRuntimeContext().getState(new ValueStateDescriptor<>("state1", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        })));
                        state2 = getRuntimeContext().getState(new ValueStateDescriptor<>("state2", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        })));
                        timeState = getRuntimeContext().getState(new ValueStateDescriptor<>("timeState", Long.class));
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        out.collect(Tuple3.of(value.f0, value.f1, "input"));
                        // 可获取第二个stream的状态，并根据逻辑删除定时器
//                        Tuple2<String, Integer> value2 = state2.value();
//                        ctx.timerService().deleteEventTimeTimer(timeState.value());
//                        timeState.clear();
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        out.collect(Tuple3.of(value.f0, value.f1, "input2"));
                        // 根据stream的元素状态
                        state2.update(value);
                        // 设定定时器的延迟执行周期
//                        long time = System.currentTimeMillis() + 60000;
//                        timeState.update(time);
//                        ctx.timerService().registerEventTimeTimer(time);
                    }

                    /**
                     * 执行定时器
                     * @param timestamp timestamp
                     * @param ctx ctx
                     * @param out out
                     * @throws Exception e
                     */
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        state1.clear();
                        state2.clear();
                    }
                })
                .print("result->").setParallelism(1);

        env.execute();
    }
}
