package com.kevin.datasource

import java.sql.{DriverManager, ResultSet}

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object MyMultipartSourceMysql {
  def main(args: Array[String]): Unit = {
    //构建流处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //添加source
    val getSource: DataStream[User] = environment.addSource(new MultipartMysqlSource).setParallelism(2)
    //处理
    getSource.setParallelism(1).print()
    environment.execute()
  }

}

class MultipartMysqlSource extends ParallelSourceFunction[User] {
  private var number = 1L
  private var isRunning = true


  override def run(sourceContext: SourceFunction.SourceContext[User]): Unit = {

    //获取链接
    val conn_str = "jdbc:mysql://localhost:3306/mydb?user=root&password=123456"
    classOf[com.mysql.jdbc.Driver]

    val conn = DriverManager.getConnection(conn_str)

    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM user")
      rs.setFetchSize(5);
      // Iterate Over ResultSet
      while (rs.next) {
        val user = new User(rs.getString("username"), rs.getInt("age"))
        sourceContext.collect(user)
      }
    }
    finally {
      conn.close
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

case class User(userName: String, age: Int) extends Serializable {
}