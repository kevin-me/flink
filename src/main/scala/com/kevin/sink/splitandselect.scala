package com.kevin.sink

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

object splitandselect {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    import org.apache.flink.api.scala._

    //构建DataStream
    val firstStream: DataStream[String] = environment.fromCollection(Array("hadoop hive","spark flink"))

    val selectStream: SplitStream[String] = firstStream.split(new OutputSelector[String] {
      override def select(out: String): lang.Iterable[String] = {
        var list = new util.ArrayList[String]()

        //如果包含hello字符串
        if (out.contains("hadoop")) {
          //存放到一个叫做first的stream里面去
          list.add("first")
        } else {
          //否则存放到一个叫做second的stream里面去
          list.add("second")
        }
        list
      }
    })

    val value: DataStream[String] = selectStream.select("frist")
    value.print()
    environment.execute()
  }

}
