package com.kevin.state

import java.lang
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object ListState {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    ))
      .keyBy(_._1)
      .flatMap(new CountAverageWithList)
      .print()
    env.execute()
  }
}

class CountAverageWithList extends RichFlatMapFunction[(Long, Double), (Long, Double)] {

  private var elementsByKey: ListState[(Long, Double)] = null

  override def open(parameters: Configuration): Unit = {

    elementsByKey = getRuntimeContext.getListState((new ListStateDescriptor[(Long, Double)]("liststate", classOf[(Long, Double)])))

  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {

    val currentState: lang.Iterable[(Long, Double)] = elementsByKey.get()

    if (currentState == null) {

      elementsByKey.addAll(Collections.emptyList())

    }

    //添加元素
    elementsByKey.add(input)

    import scala.collection.JavaConverters._
    val allElements: Iterator[(Long, Double)] = elementsByKey.get().iterator().asScala

    val allElementList: List[(Long, Double)] = allElements.toList

    if (allElementList.size >= 3) {

      var count = 0L
      var sum = 0d

      for (eachElement <- allElementList) {

        count += 1

        sum += eachElement._2
      }

      out.collect(input._1, sum / count)
    }
  }
}
