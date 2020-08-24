package com.kevin.tableandsql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{GroupWindowedTable, Table, Tumble}
import org.apache.flink.types.Row

/**
 * 基于SQL的window窗口操作处理延迟数据
 *
 * hadoop,1461756862000
 * hadoop,1461756865000
 * hadoop,1461756863000
 * hadoop,1461756868000
 * hadoop,1461756870000
 * hadoop,1461756875000
 * hadoop,1461756880000
 */
object SQLWindowWaterMark {

  //定义样例类
  case class Message(word: String, createTime: Long)

  def main(args: Array[String]): Unit = {
    //todo:1、构建流处理环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    import org.apache.flink.api.scala._

    //指定EventTime为时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //todo: 2、构建StreamTableEnvironment
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)

    //todo： 3、接受socket数据
    val sourceStream: DataStream[String] = streamEnv.socketTextStream("node01", 9999)

    //todo: 4、数据切分处理
    val mapStream: DataStream[Message] = sourceStream.map(x => Message(x.split(",")(0), x.split(",")(1).toLong))

    //todo: 5、添加watermark
    val watermarksStream: DataStream[Message] = mapStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {

      //定义延迟时长
      val maxOutOfOrderness = 5000L
      //历史最大事件时间
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        val watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        watermark
      }

      override def extractTimestamp(element: Message, previousElementTimestamp: Long): Long = {

        val eventTime: Long = element.createTime
        currentMaxTimestamp = Math.max(eventTime, currentMaxTimestamp)
        eventTime
      }
    })
    //todo:6、注册DataStream成表 ，设置时间属性
    import org.apache.flink.table.api.scala._
    tableEnvironment.registerDataStream("t_socket", watermarksStream, 'word, 'createTime.rowtime)
    //todo:7、sql查询---添加window---滚动窗口----窗口长度5s
    val result: Table = tableEnvironment.sqlQuery("select word,count(*) from t_socket group by tumble(createTime,interval '5' second),word")
    //todo:8、将table转换成DataStream
    val resultStream: DataStream[(Boolean, Row)] = tableEnvironment.toRetractStream[Row](result)

    resultStream.filter(x => x._1 == true).print()

    tableEnvironment.execute("table")
  }
}