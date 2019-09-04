package com.uek.spark.SparkCore

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 测试RDD的transFormation
  */
object Demo_05 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("demo05").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile("a.txt")

    /**
      * partitioner只有键值对RDD才拥有 一共两种 hashPartitioner RangePanrtitioner
      * 但是对于普通RDD没有partitioner
      */
    val rdd2=rdd1.flatMap(_.split(" ")).map((_,1))
    rdd2.mapPartitionsWithIndex((i,iter)=>{
      Iterator("第"+i+"个分区的数据为"+iter.mkString("[",",","]"))
    }).foreach(println(_))

    /**
      * partitionby 键值对RDD特有的算子
      * 进行数据的重分区，和自定以分区类结合使用
      * reducebykey groupbykey  也是键值对RDD特有的
      */
    rdd2.partitionBy(new CusPart(5)).mapPartitionsWithIndex(
      (i,iter)=>{
        Iterator("重分区后：第"+i+"个分区数据为"+iter.mkString("[",",","]"))
      }
    ).foreach(println(_))
    sc.stop()
  }
}

/**
  * 自定义分区器
  * 只会针对键值对RDD生效
  */
class CusPart(sum:Int) extends Partitioner{
  //需要多少个分区数量 由用户手动输入
  override def numPartitions: Int = sum
  //你的分区方式  根据你自定义的分区方式给我返回你的分区号
  //但是要注意一点，你返回的分区号总数必须和我传入的数据一致，或者大于我的分区数
  override def getPartition(key: Any): Int = {
    val str=key.toString
    if(str.startsWith("hi")){
      1
    }else if(str.startsWith("ha")){
      2
    }else if(str.startsWith("hb")){
      3
    }else if(str.startsWith("sp")){
      4
    }else{
      0
    }
  }
}
