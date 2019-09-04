package com.uek.spark.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD的转换操作一
  */
object Demo_03 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("demo03").setMaster("local[*]")
    val sc=new SparkContext(conf)
    var rdd:RDD[String]=sc.textFile("a.txt")
    //*******************第一个转换算子   map
    //map算子类似于一对一操作，就是进来一个数据就给你返回一个数据
    rdd.map((line:String)=>line+"UBDF1812").foreach(println(_))
    //****************第二个filter过滤算子
    //参数是一个函数  这个函数的返回类型是Boolean,意思是如果是true,数据
    //保留，如果是false，数据舍弃
    rdd.filter((line:String)=>if(line.startsWith("hi")) true else false).foreach(println(_))
     //************第三个算子flatmap 压扁算子
    //一对多的算子，意思是进来一行数据我给你返回一堆数据
    rdd.flatMap((line:String)=>line.split(" ")).foreach(println(_))
    //第四个算子 mapPartitionsWithIndex
    //rdd有分区，但是咱们不知道他分在哪个区，这个算子的作用就是我可以看到
    //你的分区以及分区中的数据partitioner
    rdd.mapPartitionsWithIndex((i,values)=>{
      //i是分区号，value代表每个分区中的数据
      Iterator("第"+i+"个分区的数据为："+values.mkString("[",",","]"))
    }).foreach(println(_))
    //mapPartitions,和map类似，但是map算子是一个RDD有多少条数据调用多少次函数
    //mapPartitions是有多少个分区调用多少次函数
    rdd.mapPartitions(iter=>{
      var list=List[String]()
      //iterator的一种遍历方式
      while(iter.hasNext){
        list=list:+iter.next()+"aaaa"
      }
      list.iterator
    }).foreach(println(_))
    //*************sample随机抽样
    //抽取的比例并不一定是你计算的比例，只是一个大概的范围
    var rdd1=sc.makeRDD(0 to 10000)
    rdd1.sample(true,0.001,5).foreach(println(_))
  var map=Map("zs"->2,("ls",24))


  }
}
