package com.uek.spark.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试combineBykey算子
  */
object Demo_06 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("demo05").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile("b.txt")
    val rdd2:RDD[(String,Int)]=rdd1.map(line=>{
      val words=line.split(" ")
      (words(0),words(1).toInt)
    })
    //统计最高分
    rdd2.combineByKey(value=>value,(tuple:Int,value)=>{
      if(tuple>value) tuple else value
    },(a:Int,b:Int)=>{
      if(a>b) a else b
    }).foreach(println(_))

    val rdd3:RDD[(String,(Int,Int))]=rdd2.combineByKey(value=>(value,1),
      (tuple2:(Int,Int),value)=>{
        (tuple2._1+value,tuple2._2+1)
      },(tup:(Int,Int),tup1:(Int,Int))=>{
      (tup._1+tup1._1,tup._2+tup1._2)
    })
    rdd3.map(tuple=>{
      (tuple._1,tuple._2._1/tuple._2._2)
    }).foreach(println(_))
    sc.stop()
  }
}
