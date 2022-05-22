package com.jkl.java.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @BelongProjecet flink-study-java
 * @BelongPackage com.jkl.wc
 * @Description:
 * @Author: daryl
 * @Date: 2022/4/10 15:30
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = dataStreamSource
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        wordAndOne.keyBy(data -> data.f0).sum(1).print();

        env.execute();
    }
}
