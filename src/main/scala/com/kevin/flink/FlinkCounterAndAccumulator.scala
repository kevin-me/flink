package com.kevin.flink


import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object FlinkCounterAndAccumulator {

  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._



    //统计tomcat日志当中exception关键字出现了多少次
    val sourceDataSet: DataSet[String] = env.readTextFile("C:\\words.txt")

    val value1: DataSink[String] = sourceDataSet.map(new RichMapFunction[String, String] {

      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {

        getRuntimeContext.addAccumulator("my-accumulator", counter)

      }

      override def map(value: String): String = {
        if (value.toLowerCase().contains("exception")) {
          //满足条件累加器加1
          counter.add(1)
        }
        value

      }
    }).writeAsText("c:\\c.txt")

    val job: JobExecutionResult = env.execute()
    val l: Long = job.getAccumulatorResult[Long]("my-accumulator")
    println(l)

  }

}
