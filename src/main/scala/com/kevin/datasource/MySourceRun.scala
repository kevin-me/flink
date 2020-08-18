package com.kevin.datasource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object MySourceRun {

  def main(args: Array[String]): Unit = {


    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._


    val myDatastream: DataStream[Long] = executionEnvironment.addSource(new MySource())

    val result: DataStream[Long] = myDatastream.filter(x => x % 2 == 0)

    result.print()
    executionEnvironment.execute("jishu")
  }

}

class MySource extends SourceFunction[Long] {

  private var number = 1L

  private var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {

    while (isRunning) {

      number += 1
      sourceContext.collect(number)

      Thread.sleep(1000)


    }
  }

  override def cancel(): Unit = {

    isRunning = false
  }
}