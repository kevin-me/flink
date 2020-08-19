package com.kevin.flink


import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object FlinkDistributedCache {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //准备数据集
    val scoreDataSet = env.fromElements((1, "语文", 50), (2, "数学", 60), (3, "英文", 80))

    //todo:1、注册分布式缓存文件
    env.registerCachedFile("E:\\distribute_cache_student.txt", "student")


    val value1: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      var list: List[(Int, String)] = null

      override def open(parameters: Configuration): Unit = {


        val file: File = getRuntimeContext.getDistributedCache.getFile("student")

        //获取文件的内容
        import scala.collection.JavaConverters._
        val list1: List[String] = FileUtils.readLines(file).asScala.toList

        list = list1.map(line => {
          val strings: Array[String] = line.split(".")
          (strings(0).toInt, strings(1))

        })

      }


      //在map方法中使用分布式缓存数据进行转换
      override def map(value: (Int, String, Int)): (String, String, Int) = {
        //获取学生id
        val studentId: Int = value._1
        val studentName: String = list.filter(x => studentId == x._1)(0)._2

        //封装结果返回
        // 将成绩数据(学生ID，学科，成绩) -> (学生姓名，学科，成绩)
        (studentName, value._2, value._3)

      }
    })
    value1.print()
    env.execute()
  }

}
