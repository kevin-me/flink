package com.kevin.partition

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlinkCustomerPartition {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //获取dataStream
    val sourceStream: DataStream[String] = environment.fromElements("hello laowang", "spark flink", "hello tony", "hive hadoop")

    val rePartition: DataStream[String] = sourceStream.partitionCustom(new MyPartitioner, x => x + "")
    rePartition.map(x => {
      println("数据的key为" + x + "线程为" + Thread.currentThread().getId)
      x
    })
    rePartition.print()
    environment.execute()
  }
}
