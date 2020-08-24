package com.kevin.userfull

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

case class UserLoginInfo(ip: String, username: String, operateUrl: String, time: String)

object CepCheckMyIp {

  def main(args: Array[String]): Unit = {

    /**
     * （1）输入事件流的创建
     * 就是一个DataStream
     * （2）Pattern 的定义
     * 开始定义pattern规则
     * （3）Pattern 应用在事件流上检测
     * 通过pattern规则去事件流匹配
     * （4）选取结果
     * 筛选出来
     */
    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._


    val socketStream: DataStream[String] = executionEnvironment
      .socketTextStream("node01", 9999)

    val keyStream: KeyedStream[(String, UserLoginInfo), String] = socketStream.map(x => {

      val strings: Array[String] = x.split(",")

      (strings(1), UserLoginInfo(strings(0), strings(1), strings(2), strings(3)))
    }).keyBy(_._1)


    val pattern: Pattern[(String, UserLoginInfo), (String, UserLoginInfo)] = Pattern.begin[(String, UserLoginInfo)]("start")

      .where(x => x._2 != null)
      .next("second")
      .where(new IterativeCondition[(String, UserLoginInfo)] {
        override def filter(value: (String, UserLoginInfo), context: IterativeCondition.Context[(String, UserLoginInfo)]): Boolean = {

          var flag: Boolean = false
          //获取满足前面条件的数据
          val firstValues: util.Iterator[(String, UserLoginInfo)] = context.getEventsForPattern("start").iterator()

          while (firstValues.hasNext) {

            val tuple: (String, UserLoginInfo) = firstValues.next()

            if (!tuple._2.ip.equals(value._2.ip)) {
              flag = true
            }
          }
          flag
        }
      }).within(Time.seconds(120))

    //todo:4、模式检测，将模式应用到流中

    val patternStream: PatternStream[(String, UserLoginInfo)] = CEP.pattern(keyStream, pattern)

    //todo: 5、选取结果
    patternStream.select(new MyPatternSelectFunction()).print()


    executionEnvironment.execute()
  }

}

class MyPatternSelectFunction extends PatternSelectFunction[(String, UserLoginInfo), (String, UserLoginInfo)] {
  override def select(map: util.Map[String, util.List[(String, UserLoginInfo)]]): (String, UserLoginInfo) = {

    val startIterator: util.Iterator[(String, UserLoginInfo)] = map.get("start").iterator()

    if (startIterator.hasNext) {

      println(startIterator.next())

    }

    val secondIterator: util.Iterator[(String, UserLoginInfo)] = map.get("second").iterator()


    var tuple2: (String, UserLoginInfo) = null

    if (secondIterator.hasNext) {

      tuple2 = secondIterator.next()
      println("满足条件的数据" + tuple2)

    }
    tuple2
  }
}
