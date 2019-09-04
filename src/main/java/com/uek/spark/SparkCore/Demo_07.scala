package com.uek.spark.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试aggregateByKey算子，foldByKey算子
  */
object Demo_07 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("demo07").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile("b.txt")
    val rdd2:RDD[(String,Int)]=rdd1.map(line=>{
      val words=line.split(" ")
      (words(0),words(1).toInt)
    })
    //aggreateByKey算子
    rdd2.aggregateByKey(60)((zero,value)=>
      (if(zero>value) zero else value),(one,other)=>{
      if(one>other) one else other
    }).foreach(println(_))
    rdd2.mapPartitionsWithIndex((i,iter)=>{
      Iterator("第"+i+"个分区的数据为"+iter.mkString("[",",","]"))
    }).foreach(println(_))
    rdd2.foldByKey(60)((a,b)=>a+b).foreach(println(_))
    rdd2.sortByKey()
    sc.stop()
  }
}
