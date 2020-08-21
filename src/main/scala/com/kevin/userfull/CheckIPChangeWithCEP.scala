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

object CheckIPChangeWithCEP {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //todo:1、接受socket数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)

    //todo:2、数据处理
    val keyedStream: KeyedStream[(String, UserLoginInfo), String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(1), UserLoginInfo(strings(0), strings(1), strings(2), strings(3)))
    }).keyBy(_._1)
    //todo:3、定义Pattern,指定相关条件和模型序列
    val pattern: Pattern[(String, UserLoginInfo), (String, UserLoginInfo)] = Pattern.begin[(String, UserLoginInfo)]("start")
      .where(x => x._2.username != null)
      .next("second")
      .where(new IterativeCondition[(String, UserLoginInfo)] {
        override def filter(value: (String, UserLoginInfo), context: IterativeCondition.Context[(String, UserLoginInfo)]): Boolean = {
          var flag: Boolean = false
          //获取满足前面条件的数据
          val firstValues: util.Iterator[(String, UserLoginInfo)] = context.getEventsForPattern("start").iterator()
          //遍历
          while (firstValues.hasNext) {
            val tuple: (String, UserLoginInfo) = firstValues.next()
            //ip不相同
            if (!tuple._2.ip.equals(value._2.ip)) {
              flag = true
            }
          }

          flag
        }
      })
      //可以指定模式在一段时间内有效
      .within(Time.seconds(120))
    //todo:4、模式检测，将模式应用到流中
    val patternStream: PatternStream[(String, UserLoginInfo)] = CEP.pattern(keyedStream, pattern)

    //todo: 5、选取结果
    patternStream.select(new MyPatternSelectFunction).print()

    //todo: 6、开启计算
    environment.execute()

  }

  //自定义PatternSelectFunction类
  class MyPatternSelectFunction extends PatternSelectFunction[(String, UserLoginInfo), (String, UserLoginInfo)] {
    override def select(map: util.Map[String, util.List[(String, UserLoginInfo)]]): (String, UserLoginInfo) = {
      // 获取Pattern名称为start的事件
      val startIterator = map.get("start").iterator()

      if (startIterator.hasNext) {
        println("满足start模式中的数据：" + startIterator.next())
      }
      //获取Pattern名称为second的事件
      val secondIterator = map.get("second").iterator()

      var tuple: (String, UserLoginInfo) = null

      if (secondIterator.hasNext) {
        tuple = secondIterator.next()
        println("满足second模式中的数据：" + tuple)
      }

      tuple
    }
  }

}