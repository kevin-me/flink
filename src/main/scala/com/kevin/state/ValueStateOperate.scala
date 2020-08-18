package com.kevin.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object ValueStateOperate {

  def main(args: Array[String]): Unit = {

    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._


    executionEnvironment.fromCollection(
      List(
        (1L, 3d),
        (1L, 5d),
        (1L, 7d),
        (1L, 4d),
        (1L, 2d))).keyBy(_._1).flatMap(new CountAverageWithValue()).print()

    executionEnvironment.execute()


  }

}

class CountAverageWithValue extends RichFlatMapFunction[(Long, Double), (Long, Double)] {


  private var sum: ValueState[(Long, Double)] = null

  //进行初始化
  override def open(parameters: Configuration): Unit = {

    sum = getRuntimeContext.getState(new ValueStateDescriptor[(Long, Double)]("average", classOf[(Long, Double)]))
  }

  /**
   *
   * @param input
   * @param out
   */
  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {

    val tmpCurrentSum: (Long, Double) = sum.value

    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0D)
    }
    //更新state 的值

    println("*******"+currentSum)

    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    sum.update(newSum)

    if (newSum._1 >=2) {

      out.collect(input._1, newSum._2 / newSum._1)

     // sum.clear()
    }

  }
}