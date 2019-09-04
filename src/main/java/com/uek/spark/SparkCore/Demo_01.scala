package com.uek.spark.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试创建rdd 方法一：从textfile外部数据源创建
  */
object Demo_01 {
  def main(args: Array[String]): Unit = {
    //构建程序入口
    val conf=new SparkConf().setAppName("demo01").setMaster("local[*]")
    val sc=new SparkContext(conf)
    //创建rdd
    val rdd1:RDD[String]=sc.textFile("a.txt")
    rdd1.foreach(println(_))
    sc.stop()
  }
}
