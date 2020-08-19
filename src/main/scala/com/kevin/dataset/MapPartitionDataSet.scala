package com.kevin.dataset

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

object MapPartitionDataSet {

  def main(args: Array[String]): Unit = {

    val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val arrayBuffer = new ArrayBuffer[String]()

    arrayBuffer.+=("hello word1")

    arrayBuffer.+=("hello word2")

    arrayBuffer.+=("hello word3")

    arrayBuffer.+=("hello word4")

    val arrayStream: DataSet[String] = executionEnvironment.fromCollection(arrayBuffer)


    val value: DataSet[String] = arrayStream.mapPartition(eachPartition => {

      eachPartition.map(eachLine => {

        val str: String = eachLine + "result"
        str

      })

    })
    value.print()

    val value1: DataSet[String] = arrayStream.flatMap(z => z.split(" ")).distinct()

    value1.print()


  }

}
