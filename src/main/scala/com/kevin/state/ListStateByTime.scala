package com.kevin.state

import java.{lang, util}
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import java.util.Date
import java.text.SimpleDateFormat


/**
 * 作业
 * 传输数据  userid,time
 *
 *
 */
object ListStateByTime {

  def main(args: Array[String]): Unit = {

    //构建流处理的环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从socket获取数据
    val sourceStream: DataStream[String] = env.socketTextStream("node01", 9999)
    //导入隐式转换的包
    import org.apache.flink.api.scala._

    //数据  "tony", "2020-08-18 13:06:08"
    //     "tony", "2020-08-18 13:06:07"

    val userTime: DataStream[String] = sourceStream.flatMap(x => x.split(" "))

    userTime.print()


    val spilt: DataStream[(String, String)] = userTime.map(x => (x.split(",")(0), x.split(",")(1)))

    spilt.print()

    spilt.keyBy(_._1).
      flatMap(new UserTimeWithList).print()

    //    env.fromCollection(List(
    //      ("tony", "2020-08-18 13:06:08"),
    //      ("tony", "2020-08-18 13:07:08"),
    //      ("tony", "2020-08-18 13:10:08"),
    //      ("kevin", "2020-08-18 13:05:08"),
    //      ("kevin", "2020-08-18 13:09:08"),
    //      ("kevin", "2020-08-18 13:10:08")
    //    ))
    //      .keyBy(_._1)
    //      .flatMap(new UserTimeWithList)
    //      .print()

    env.execute()
  }

}

class UserTimeWithList extends RichFlatMapFunction[(String, String), (String, Long)] {

  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private var elementsByKey: ListState[(String, Long)] = null

  override def open(parameters: Configuration): Unit = {

    elementsByKey = getRuntimeContext.getListState((new ListStateDescriptor[(String, Long)]("usertime", classOf[(String, Long)])))

  }

  override def flatMap(input: (String, String), out: Collector[(String, Long)]): Unit = {

    val currentState: lang.Iterable[(String, Long)] = elementsByKey.get()

    if (currentState == null) {

      elementsByKey.addAll(Collections.emptyList())

    }
    //解析时间
    val actionTime = TIME_FORMAT.parse(input._2)
    // 计算毫秒值
    val mTime = actionTime.getTime()

    //添加元素
    elementsByKey.add((input._1, mTime))

    println("user + long *********" + (input._1, mTime))

    import scala.collection.JavaConverters._

    val allElements: Iterator[(String, Long)] = elementsByKey.get().iterator().asScala

    val allElementList: List[(String, Long)] = allElements.toList

    if (allElementList.size >= 3) {
      //按照时间排序
      allElementList.sortBy(s => (s._1(1)))
      //前后时间相减
      val firstInterval = allElementList(1)._2 - allElementList(0)._2

      val secondInterval = allElementList(2)._2 - allElementList(1)._2
      //取时间的最小值
      if (firstInterval > secondInterval) {
        out.collect(input._1, secondInterval)
      } else {
        out.collect(input._1, firstInterval)
      }
    }
  }
}