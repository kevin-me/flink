package com.kevin.tableandsql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
101,zhangsan,18
102,lisi,20
103,wangwu,25
104,zhaoliu,15
 */
object FlinkSQLTest {

  //todo:定义样例类
  case class User(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //todo:1、构建流处理环境
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    //todo:2、构建TableEnvironment
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)
    import org.apache.flink.api.scala._

    /**
     * 101,zhangsan,18
     * 102,lisi,20
     * 103,wangwu,25
     * 104,zhaoliu,15
     */
    //todo:3、接受socket数据
    val socketStream: DataStream[String] = streamEnv.socketTextStream("node01", 9999)

    //todo:4、对数据进行处理
    val userStream: DataStream[User] = socketStream.map(x => x.split(",")).map(x => User(x(0).toInt, x(1), x(2).toInt))

    //todo:5、将流注册成一张表
    tableEnvironment.registerDataStream("userTable", userStream)

    //todo:6、使用table 的api查询年龄大于20岁的人
    val result: Table = tableEnvironment.sqlQuery("select * from userTable where age >20")
    //todo：7、将table转化成流
    tableEnvironment.toAppendStream[Row](result).print()
    //todo:8、启动
    tableEnvironment.execute("TableFromDataStream")

  }

}