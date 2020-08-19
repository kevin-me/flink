package com.kevin.flink

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ArrayBuffer

object FlinkBroadCast {

  def main(args: Array[String]): Unit = {
    val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val userInfo = ArrayBuffer(("zs", 10), ("ls", 20), ("ww", 30))
    val userDataSet: DataSet[(String, Int)] = executionEnvironment.fromCollection(userInfo)
    //原始数据
    val data = executionEnvironment.fromElements("zs", "ls", "ww")
    data.map(new RichMapFunction[String, String] {
      var listData: java.util.List[(String, Int)] = null
      var map = Map[String, Int]()
      override def open(parameters: Configuration): Unit = {
        listData = getRuntimeContext.getBroadcastVariable[(String, Int)]("broadcastMapName")
        val it: util.Iterator[(String, Int)] = listData.iterator()
        while (it.hasNext) {
          val tuple: (String, Int) = it.next()
          map += (tuple._1 -> tuple._2)
        }
      }
      override def map(name: String): String = {
        val i: Int = map.getOrElse(name, 20)
        name + "," + i
      }
    }).withBroadcastSet(userDataSet, "broadcastMapName")
  }
}
