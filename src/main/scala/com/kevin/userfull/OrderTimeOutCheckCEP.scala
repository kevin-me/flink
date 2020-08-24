package com.kevin.userfull

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream, pattern}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 订单下单未支付检测
 */

case class OrderDetail(orderId: String, status: String, orderCreateTime: String, price: Double)

object OrderTimeOutCheckCEP {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)

    val keyedStream: KeyedStream[OrderDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      OrderDetail(strings(0), strings(1), strings(2), strings(3).toDouble)

    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
      override def extractTimestamp(element: OrderDetail): Long = {
        format.parse(element.orderCreateTime).getTime
      }
    }).keyBy(x => x.orderId)

    //定义Pattern模式，指定条件
    val pattern: Pattern[OrderDetail, OrderDetail] = Pattern.begin[OrderDetail]("start")
      .where(order => order.status.equals("1"))
      .followedBy("second")
      .where(x => x.status.equals("2"))
      .within(Time.minutes(15))


    // 4. 调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderDetail]("orderTimeout")

    val patternStream: PatternStream[OrderDetail] = CEP.pattern(keyedStream, pattern)
    val selectResultStream: DataStream[OrderDetail] = patternStream
      .select(orderTimeoutOutputTag, new OrderTimeoutPatternFunction, new OrderPatternFunction)

    selectResultStream.print()

    //打印侧输出流数据 过了15分钟还没支付的数据
    selectResultStream.getSideOutput(orderTimeoutOutputTag).print()
    environment.execute()
  }
}

//订单超时检测
class OrderTimeoutPatternFunction extends PatternTimeoutFunction[OrderDetail, OrderDetail] {

  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], l: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    println("超时订单号为" + detail)
    detail
  }
}


class OrderPatternFunction extends PatternSelectFunction[OrderDetail, OrderDetail] {
  override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail: OrderDetail = pattern.get("second").iterator().next()
    println("支付成功的订单为" + detail)
    detail
  }
}
