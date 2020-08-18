package com.kevin.datasource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamingSourceFromFile {

  def main(args: Array[String]): Unit = {

    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val wordDataStream: DataStream[String] = executionEnvironment
      .readTextFile("c:\\words.txt")

    import org.apache.flink.api.scala._

    val result: DataStream[(String, Int)] = wordDataStream.flatMap(x => x.split(" ")).map((_, 1)).keyBy(0).sum(1)

    result.setParallelism(4).writeAsText("c:\\b.txt")

    executionEnvironment.execute("flinkwordCount")

  }

}
