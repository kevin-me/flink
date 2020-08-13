package com.kevin.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


object FlinkStream {

  def main(args: Array[String]): Unit = {

    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream: DataStream[String] = executionEnvironment.socketTextStream("node01", 9999)

    //导入隐式转换包
    import org.apache.flink.api.scala._

    val wordDataStream: DataStream[(String, Int)] = sourceStream.flatMap(x => x.split(" ")).map((_, 1)).keyBy(0).
      timeWindow(Time.seconds(2),Time.seconds(1)) //每隔1s处理2s的数据
      .sum(1)

    wordDataStream.print()

    executionEnvironment.execute("FlinkStream")
  }
}
