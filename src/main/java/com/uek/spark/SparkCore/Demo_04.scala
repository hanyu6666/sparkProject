package com.uek.spark.SparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Demo_04 {
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("demo4").setMaster("local[*]")
    var sc=new SparkContext(conf)
    var rdd=sc.makeRDD(List((0,List("a","b","c")),(1,List("d","e","f"))))
    println(rdd.preferredLocations(rdd.partitions(0)))
    //如果你在创建RDD的时候没有指定分区数，那么他会找你setMaster(local[3])
    //里面设置的总核数就是你的分区数
    var rdd1=sc.makeRDD(List((1,2),(1,4),(2,6)))
    rdd1.mapPartitionsWithIndex((i,iter)=>{
      Iterator("第"+i+"个分区的数据为"+iter.mkString("[",",","]"))
    }).foreach(println(_))
    rdd1.reduceByKey(_+_,5).foreach(println(_))
    sc.stop()
  }
}
