package com.kevin.partition

import org.apache.flink.api.common.functions.Partitioner

class MyPartitioner extends Partitioner[String] {
  override def partition(line: String, num: Int): Int = {
    println("分区个数为" + num)
    if (line.contains("hello")) {
      0
    } else {
      1
    }
  }
}
