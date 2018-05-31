package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jiangxinlin
 * @version 2018-05-31
 */
@Component
public class SparkDemo implements Serializable {


    public static void main(String[] args) {

        /**

         * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，

         * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置

         * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如

         * 只有1G的内存）的初学者       *

         */
        SparkConf conf = new SparkConf().setAppName("Spark WordCount written by java").setMaster("local");


        /**

         * 第2步：创建SparkContext对象

         * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext(不同的语言具体的类名称不同，如果是java 的为javaSparkContext)

         * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend

         * 同时还会负责Spark程序往Master注册程序等

         * SparkContext是整个Spark应用程序中最为至关重要的一个对象

         */
        JavaSparkContext sc = new JavaSparkContext(conf); //其底层就是scala的sparkcontext


        /**

         * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD

         * JavaRDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作

         * 数据会被JavaRDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴

         */
        JavaRDD<String> lines = sc.textFile("d:\\hadoop\\wc.txt").cache();


        /**

         * 第4步：对初始的JavaRDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算

         */
        // 第4.1步：讲每一行的字符串拆分成单个的单词
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split("\\s+")));


        // 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));

        // 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer s, Integer t) -> s + t);

        // 结果输出
        List<Tuple2<String, Integer>> output = counts.collect();

        // put到map
        Map<String, Integer> map = new HashMap<>();
        for (Tuple2<String, Integer> tuple : output) {
            map.put(tuple._1(), tuple._2());
        }
        System.out.println(map);
    }

}
