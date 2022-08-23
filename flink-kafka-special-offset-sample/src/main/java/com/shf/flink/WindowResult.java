package com.shf.flink;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * description :
 * {@link UserCountAggregateFunction}结果收集
 *
 * @author songhaifeng
 * @date 2022/8/16 11:22
 */
@Slf4j
public class WindowResult extends ProcessWindowFunction<Long, Tuple4<Integer, Long, Long, Long>, Integer, TimeWindow> {
    @Override
    public void process(Integer userId, Context context, Iterable<Long> elements, Collector<Tuple4<Integer, Long, Long, Long>> out) {
        TimeWindow timeWindow = context.window();
        ArrayList<Long> events = Lists.newArrayList(elements);
        log.info("collect {} events", events.size());
        // window的开始和截止时间戳一并输出
        out.collect(new Tuple4<>(userId, events.get(0), timeWindow.getStart(), timeWindow.getEnd()));
    }
}
