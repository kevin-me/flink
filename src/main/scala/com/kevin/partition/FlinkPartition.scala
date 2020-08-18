package com.kevin.partition
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 4.7 重分区算子
 *
 * - ==重算子允许我们对数据进行重新分区，或者解决数据倾斜等问题==
 *   - Random Partitioning
 *     - 随机分区
 *       - 根据均匀分布随机分配元素（类似于random.nextInt(5)，0 - 5 在概率上是均匀的）
 *       - dataStream.shuffle()
 *   - Rebalancing
 *     - 均匀分区
 *       - 分区元素循环，每个分区创建相等的负载。数据发生倾斜的时候可以用于性能优化。
 *       - 对数据集进行再平衡，重分区，消除数据倾斜
 *       - dataStream.rebalance()
 *   - Rescaling：
 *     - 跟rebalance有点类似，但不是全局的，这种方式仅发生在一个单一的节点，因此没有跨网络的数据传输。
 *       - dataStream.rescale()
 *   - Custom partitioning：自定义分区
 *     - 自定义分区需要实现Partitioner接口
 *       - dataStream.partitionCustom(partitioner, "someKey")
 *       - 或者dataStream.partitionCustom(partitioner, 0);
 *   - Broadcasting：广播变量，后面详细讲解
 */

object FlinkPartition {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._


    val dataStream: DataStream[Int] = environment.fromCollection(1 to 100)

    val filterStream: DataStream[Int] = dataStream.filter(x => x>10)
      //.shuffle  //随机的重新分发数据,上游的数据，随机的发送到下游的分区里面去
      //.rescale
      .rebalance //对数据重新进行分区，涉及到shuffle的过程


    val resultStream: DataStream[(Int, Int)] = filterStream.map(new RichMapFunction[Int, (Int, Int)] {
      override def map(value: Int): (Int, Int) = {
        //获取任务id，以及value
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })
    resultStream.print()
    environment.execute()


  }

}
