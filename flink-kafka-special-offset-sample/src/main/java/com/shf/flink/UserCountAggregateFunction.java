package com.shf.flink;

import com.shf.flink.event.UserBehaviorEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * description :
 * 每个用户的行为累加
 *
 * @author songhaifeng
 * @date 2022/8/16 11:20
 */
public class UserCountAggregateFunction implements AggregateFunction<UserBehaviorEvent, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehaviorEvent userBehaviorEvent, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
