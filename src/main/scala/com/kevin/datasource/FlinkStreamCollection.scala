package com.kevin.datasource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlinkStreamCollection {

  def main(args: Array[String]): Unit = {

    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val array = Array("dada dad", "dada", "acvada")

    //导入隐式转换的包
    import org.apache.flink.api.scala._

    val fromArray: DataStream[String] = executionEnvironment.fromCollection(array)

    //  val value: DataStream[String] = environment.fromElements("hello world")
    val resultDataStream: DataStream[(String, Int)] = fromArray
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)
    //打印
    resultDataStream.print()
    //启动
    executionEnvironment.execute()

  }

}
