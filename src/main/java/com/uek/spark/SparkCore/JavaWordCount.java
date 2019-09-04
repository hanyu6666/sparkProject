package com.uek.spark.SparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import sun.applet.Main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * JavaApi
 * RDD的Transformation转换算子
 */
public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("wc").setMaster("local[*]");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = jsc.textFile("a.txt");
        //1.使用空格分隔符切
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                String[] words = s.split(" ");
                for (String word : words) {
                    list.add(word);
                }
                return list.iterator();
            }
        });
        //2.打标签
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //3.根据key值聚合
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //4.查看每个分区和数据
        rdd4.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<String>>() {

            @Override
            public Iterator<String> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
                List<String> list=new ArrayList<String>();
                list.add("第"+v1+"分区数据为"+v2);
                return list.iterator();
            }
        },false).foreach(line->System.out.println(line));
        JavaPairRDD<String, Iterable<Integer>> rdd5 = rdd3.groupByKey();
        rdd5.foreach(tuple->{
            String word=tuple._1;
            Iterable<Integer> integers=tuple._2;
            System.out.println("key是："+word+"---------对应值为"+integers);
        });

    }
}
