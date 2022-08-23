/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.shf.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;

@Slf4j
public class StreamingWithSideOutputJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 建议通过启动命令参数设置
        env.setRuntimeMode(BATCH);

        DataStream<String> text = env.fromElements("a b dfghyr c d asdada g t a c d asdada b s a x dfghyr a c b t s asdada");
        // 字符长度大于5则reject
        OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {
        };

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] tokens = value.toLowerCase().split("\\W+");

                        for (String token : tokens) {
                            // 长度大于5的token加入到旁路流中
                            if (token.length() > 5) {
                                ctx.output(rejectedWordsTag, token);
                            } else if (token.length() > 0) {
                                // 反之保留在主流中
                                out.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                });

        // wordCount主流
        DataStream<Tuple2<String, Integer>> counts = tokenized.setParallelism(3)
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(value -> value.f0)
                .sum(1);
        counts.print();

        // 处理旁路流
        DataStream<Tuple2<String, Integer>> rejectedWords = tokenized
                .getSideOutput(rejectedWordsTag)
                .map(word -> Tuple2.of("rejected: " + word, 1), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                .keyBy(value -> value.f0)
                .sum(1);
        rejectedWords.print();

        env.execute("Streaming WordCount SideOutput Job");
    }

}
