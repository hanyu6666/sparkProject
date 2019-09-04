package com.uek.spark.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试创建RDD，方法二：从scala集合创建
  */
object Demo_02 {
  def main(args: Array[String]): Unit = {
    //1.构建程序入口
    val conf=new SparkConf().setAppName("demo02").setMaster("local[*]")
    val sc=new SparkContext(conf)
    //2.创建rdd
    //**********第一种并行化构建
    val rdd:RDD[Any]=sc.parallelize(Array(1,2,5,"a"))
    rdd.foreach(println(_))
    //************第二种 通过makerdd构建
    //makerdd第一种方式  底层还是通过parallelize去实现
    val rdd1:RDD[Int]=sc.makeRDD(List(1,2,3))
    rdd1.foreach(println(_))
    //makerdd第二种方式构建，会指定你将那些数据存放到哪个机器的分区上
    val rdd2=sc.makeRDD(List((0,List("aaaaa")),(1,List("a","b","c"))))
    val strings=rdd2.preferredLocations(rdd2.partitions(0))
    println(strings)
    sc.stop()



  }
}
