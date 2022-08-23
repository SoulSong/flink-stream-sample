package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description :
 * 使用countTrigger(2):
 * 接收两个元素即触发窗口计算，由于countWindow是GlobalWindow，没有window销毁的操作，会持续累计窗口的state
 * 7> (bar,2)
 * 7> (bar,4)
 * 3> (foo,2)
 * 3> (foo,4)
 * 3> (foo,6)
 * <p>
 * 使用PurgingTrigger.of(CountTrigger.of(2)):
 * 接收两个元素即触发窗口计算，同时清除上一个窗口的计算状态，从而实现每个窗口独立计算
 * 3> (foo,2)
 * 3> (foo,2)
 * 7> (bar,2)
 * 3> (foo,2)
 * 7> (bar,2)
 * <p>
 * 使用PurgingTrigger.of(CountTrigger.of(2)).evictor(CountEvictor.of(1))：
 * 每个窗口仅保留最近的1个元素
 * <p>
 * 7> (bar,1)
 * 7> (bar,1)
 * 3> (foo,1)
 * 3> (foo,1)
 * 3> (foo,1)
 *
 * @author songhaifeng
 * @date 2022/8/23 15:22
 */
@Slf4j
public class CountTriggerJob {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 5);
        final long triggerSize = params.getLong("triggerSize", 2);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<Tuple2<String, Integer>> input = new ArrayList<>();
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(BAR, 1));
        input.add(Tuple2.of(FOO, 1));
        input.add(Tuple2.of(FOO, 1));

        env.fromCollection(input)
                .keyBy(event -> event.f0)
                // countWindow是基于trigger+globalWindow实现
                .countWindow(windowSize)
                // trigger会覆盖countWindow中默认的trigger
//                .trigger(CountTrigger.of(triggerSize))
                // 清除上一个窗口触发计算的state
                .trigger(PurgingTrigger.of(CountTrigger.of(triggerSize)))
                // 每个窗口仅保留最近的1个元素
                .evictor(CountEvictor.of(1))
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1),
                        // 每次trigger均会执行reduce计算，并将结果输出至precessWindowFunction
                        new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, GlobalWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                                log.info("key -> {} ; elements -> {}", s, elements);
                                out.collect(elements.iterator().next());
                            }
                        })
                .print();

        env.execute();
    }

}
