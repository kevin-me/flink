package com.kevin.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 数据类型必须一致 可以多次 union
 */
object UnionStream {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val firstStream: DataStream[String] = environment.fromCollection(Array("hello spark", "hello flink"))
    val secondStream: DataStream[String] = environment.fromCollection(Array("hadoop spark", "hive flink"))

    val third: DataStream[String] = firstStream.union(secondStream)

    third.print()
    environment.execute()

  }

}
