package com.jkl.java.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("input/test.txt");

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = dataSource
                .flatMap(
                        (String line, Collector<Tuple2<String, Long>> out) -> {
                            //将一行按照空格进行切割单词
                            String[] words = line.split(" ");
                            //将每个单词转换为二元组输出
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        })
                //flink编译后会进行泛型擦除，无法知道tuple里面具体是什么类型，所以此处需要手动声明类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        wordAndOne.groupBy(0)
                .sum(1)
                .print();

    }
}
