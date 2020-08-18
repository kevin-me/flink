package com.kevin.sink

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，
 * 会对两个流中的数据应用不同的处理方法
 */
object ConnectStream {

  def main(args: Array[String]): Unit = {

    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._


    val s1: DataStream[String] = executionEnvironment.fromCollection(Array("aaa", "cvvv"))


    val s2: DataStream[Int] = executionEnvironment.fromCollection(Array(1, 2))

    val s3: ConnectedStreams[String, Int] = s1.connect(s2)

    val unionStream: DataStream[Any] = s3.map(x => x + "abc", y => y * 2)

    val value: DataStream[String] = s3.flatMap(new CoFlatMapFunction[String, Int, String] {
      override def flatMap1(in1: String, collector: Collector[String]): Unit = {
        collector.collect(in1 + "ddd")
      }

      override def flatMap2(in2: Int, collector: Collector[String]): Unit = {
        collector.collect(in2 + 2 + "")
      }
    })
    value.print()

    unionStream.print()


    executionEnvironment.execute()

  }

}
