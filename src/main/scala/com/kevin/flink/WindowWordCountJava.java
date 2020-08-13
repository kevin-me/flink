package com.kevin.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCountJava
{
    public static void main(String[] args) throws Exception
    {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> socketTextStream = executionEnvironment.socketTextStream("node01", 9999);


        DataStream<WordCount> wordCountSingleOutputStreamOperator = socketTextStream.flatMap(new FlatMapFunction<String, WordCount>()
        {

            public void flatMap(String line, Collector<WordCount> collector) throws Exception
            {
                String[] split = line.split(" ");

                for (String word : split)
                {
                    collector.collect(new WordCount(word, 1L));

                }

            }
        });

        DataStream<WordCount> sum = wordCountSingleOutputStreamOperator.keyBy("word")  //按照单词分组
                .timeWindow(Time.seconds(2), Time.seconds(1)) //每隔1s统计2s的数据
                .sum("count");//按照count字段累加结果

        sum.print();

        executionEnvironment.execute("streamJava");

    }

    public static class WordCount
    {
        public String word;
        public long count;

        //记得要有这个空构建
        public WordCount()
        {

        }

        public WordCount(String word, long count)
        {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString()
        {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

    }

}
