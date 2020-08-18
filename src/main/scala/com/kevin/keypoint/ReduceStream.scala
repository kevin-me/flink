package com.kevin.keypoint

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object ReduceStream {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[(String, Int)] = environment
      .fromElements(("a", 1), ("a", 2), ("b", 2), ("a", 3), ("c", 2),("c", 2))

    val keyByStream: KeyedStream[(String, Int), Tuple] = sourceStream.keyBy(0)

    //  val resultStream: DataStream[(String, Int)] = keyByStream.reduce((t1,t2)=>(t1._1,t1._2+t2._2))

    val resultStream: DataStream[(String, Int)] = keyByStream.reduce((t1, t2) => (t1._1, t2._2 + t1._2))

    resultStream.setParallelism(1).print()

    environment.execute()

  }

}
