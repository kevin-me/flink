package com.kevin

import com.alibaba.fastjson.JSON
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction


/**
 * CountEvictor移除器的使用
 *
 * 测试数据：
 *  某个用户在某个时刻浏览了某个商品，以及商品的金额
 *  {"userID": "user_4", "eventTime": "2019-11-09 10:41:32", "productID": "product_1", "productPrice": 10}
 *
 *
 * {"userID": "user_1", "eventTime": "2019-11-09 10:41:32", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_1", "eventTime": "2019-11-09 10:41:33", "productID": "product_2", "productPrice": 20}
 * {"userID": "user_1", "eventTime": "2019-11-09 10:41:34", "productID": "product_3", "productPrice": 30}
 * {"userID": "user_1", "eventTime": "2019-11-09 10:41:35", "productID": "product_4", "productPrice": 40}
 * {"userID": "user_1", "eventTime": "2019-11-09 10:41:36", "productID": "product_5", "productPrice": 50}
 * {"userID": "user_1", "eventTime": "2019-11-09 10:41:37", "productID": "product_5", "productPrice": 60}
 *
 */

case class UserActionLog(userID:String,eventTime:String,productID:String,productPrice:Double)

object CountEvictorDemo {
  def main(args: Array[String]): Unit = {
    //todo: 构建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //todo: 接受socket数据
    val socketSource: DataStream[String] = environment.socketTextStream("node01",9999)

    //todo: 数据处理
    //解析json字符串
    socketSource.map(jsonStr =>json2Objecct(jsonStr))
      //按照userID分组
      .keyBy(_.userID)
      //构建timeWindow
      .timeWindow(Time.seconds(3))
      // 基于ProcessingTime触发器
      .trigger(ProcessingTimeTrigger.create())
      //计数剔除器 表示：Window Function执行前从Window中剔除元素,剔除后Window中只保留3个元素
      .evictor(CountEvictor.of(3,false))
      //窗口函数，对窗口内的数据进行处理
      .process(new CustomProcessWindowFunction)
      .print()



    environment.execute("CountEvictorDemo")
  }

  /**
   * json字符串转换为对象
   * @param jsonStr
   * @return
   */
  def json2Objecct(jsonStr: String):UserActionLog={
    val userActionLog: UserActionLog = JSON.parseObject[UserActionLog](jsonStr, classOf[UserActionLog])
    userActionLog
  }

}

//定义ProcessWindowFunction类
class CustomProcessWindowFunction extends ProcessWindowFunction[UserActionLog,String,String,TimeWindow]{

  /**
   * 对窗口的数据进行聚合处理
   *
   * @param key      分组的字段类型
   * @param context  窗口中的上下文对象
   * @param elements 窗口内的数据
   * @param out      输出的结果类型
   */
  override def process(key: String, context: Context, elements: Iterable[UserActionLog], out: Collector[String]): Unit = {
    //定义一个可变的字符序列
    val allRecords = new StringBuilder
    import scala.collection.JavaConversions._
    //遍历
    for (element <- elements) {
      //添加元素
      allRecords.append(element).append("\n")
    }

    val windowStart: Long = context.window.getStart
    val windowEnd: Long = context.window.getEnd
    val result: String = "Key: " + key + " 窗口开始时间: " + windowStart + " 窗口结束时间: " + windowEnd + " 窗口所有数据: \n" + allRecords

    //输出结果
    out.collect(result)
  }
}
