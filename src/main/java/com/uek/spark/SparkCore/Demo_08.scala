package com.uek.spark.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount
  */
object Demo_08 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Demo08").setMaster("local[*]")
    val sc=new SparkContext(conf)
    //sc.setCheckpointDir("./")
    val rdd1:RDD[String]=sc.textFile("a.txt")
    rdd1.cache()
    /*rdd1.persist(StorageLevel.MEMORY_AND_DISK_2)*/
    //1.将一行一行的数据按照空格为切割符转换成一个一个的单词
    val rdd2:RDD[String]=rdd1.flatMap((line:String)=>line.split(" "))
    //2.将单词打上标签(word,1)
    val rdd3:RDD[(String,Int)]=rdd2.map((_,1))
    //3.按照单词进行计数
    val rdd4:RDD[(String,Int)]=rdd3.reduceByKey(_+_)
    //RDD的行动操作
    val array:Array[(String,Int)]=rdd4.collect()
    array.foreach(println(_))
  }

}
