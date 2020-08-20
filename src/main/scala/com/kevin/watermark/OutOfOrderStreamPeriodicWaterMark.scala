import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
000001,1461756862000
000001,1461756866000
000001,1461756868000
000001,1461756869000
000001,1461756870000
000001,1461756862000
000001,1461756871000
000001,1461756872000
000001,1461756862000
000001,1461756863000
 * 使用周期性的生成watermark解决数据的延迟或者乱序，统计时间窗口大小为5s的每个用户出现的次数，允许最大的延迟时间3s，并且使用侧输出流收集延迟太久的数据。
 */

//对无序的数据流周期性的添加水印
object OutOfOrderStreamPeriodicWaterMark {
  def main(args: Array[String]): Unit = {
    //构建流式处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    environment.setParallelism(1)

    //设置时间类型
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //获取数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("node01", 9999)

    //数据处理
    val mapStream: DataStream[(String, Long)] = sourceStream.map(x => (x.split(",")(0), x.split(",")(1).toLong))

    //定义一个侧输出流的标签，用于收集迟到太多的数据
    val lateTag=new OutputTag[(String, Long)]("late")

    //添加水位线
    val result: DataStream[(String, Long)] = mapStream.assignTimestampsAndWatermarks(

      new AssignerWithPeriodicWatermarks[(String, Long)] {

        //定义延迟时间长度
        //表示在3秒以内的数据延时有效，超过3秒的数据被认定为迟到事件
        val maxOutOfOrderness = 3000L

        //历史最大事件时间
        var currentMaxTimestamp: Long = _

        var watermark: Watermark = _

        //周期性的生成水位线watermark
        override def getCurrentWatermark: Watermark = {
          watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
          watermark
        }

        //抽取事件时间
        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          //获取事件时间
          val currentElementEventTime: Long = element._2

          //对比当前事件时间和历史最大事件时间, 将较大值重新赋值给currentMaxTimestamp
          currentMaxTimestamp = Math.max(currentMaxTimestamp, currentElementEventTime)
          println("接受到的事件：" + element + " |事件时间： " + currentElementEventTime)

          currentElementEventTime
        }

      })
      .keyBy(0)

      .timeWindow(Time.seconds(5)) //5秒的数据
      .sideOutputLateData(lateTag)
      .process(
        /**
         * 每一个参数的含义
         */
        new ProcessWindowFunction[(String, Long), (String, Long), Tuple, TimeWindow] {

        override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {

          val value: String = key.getField[String](0)
          //窗口的开始时间
          val startTime: Long = context.window.getStart
          //窗口的结束时间
          val startEnd: Long = context.window.getEnd

          //获取当前的 watermark
          val watermark: Long = context.currentWatermark

          var sum: Long = 0

          val toList: List[(String, Long)] = elements.toList
          for (eachElement <- toList) {
            sum += 1
          }
          println("窗口的数据条数:" + sum +
            " |窗口的第一条数据：" + toList.head +
            " |窗口的最后一条数据：" + toList.last +
            " |窗口的开始时间： " + startTime +
            " |窗口的结束时间： " + startEnd +
            " |当前的watermark:" + watermark)
          out.collect((value, sum))
        }
      })
    //打印延迟太多的数据
    result.getSideOutput(lateTag).print("late")

    //打印
    result.print("ok")
  }
}