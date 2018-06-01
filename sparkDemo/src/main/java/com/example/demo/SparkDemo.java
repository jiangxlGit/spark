package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * @author jiangxinlin
 * @version 2018-05-31
 */
public class SparkDemo implements Serializable {


    public transient JavaSparkContext sc = null;

    @Before
    public void before() {
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
        sc = new JavaSparkContext(conf); //其底层就是scala的sparkcontext
    }

    @Test
    public void wordCount() {


        /**

         * 第3步：根据具体的数据来源（HDFS、HBase、Local FS、DB、S3等）通过SparkContext来创建RDD

         * JavaRDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其它的RDD操作

         * 数据会被JavaRDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴

         */
        JavaRDD<String> lines_1 = sc.textFile("d:\\hadoop\\wc_1.txt").cache();
        JavaRDD<String> lines_2 = sc.textFile("d:\\hadoop\\wc_2.txt").cache();

        lines_1 = lines_1.distinct();

        // 合并rdd
        JavaRDD<String> lines = lines_1.union(lines_2);

        /**

         * 第4步：对初始的JavaRDD进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算

         */
        // 第4.1步：讲每一行的字符串拆分成单个的单词
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split("^A-Za-z0-9_|\\s+")));

        // 相同只保留一个元素
//        words = words.distinct();

        // 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        // 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer s, Integer t) -> s + t);

        // 结果输出
        List<Tuple2<String, Integer>> output = counts.collect();

        // put到map
        Map<String, Integer> map = new HashMap<>();
        for (Tuple2<String, Integer> tuple : output) {
            map.put(tuple._1(), tuple._2());
        }

        // 打印map
        System.out.println(map);

        // 统计出出现最多的单词
        Iterator it = map.entrySet().iterator();
        Integer maxValue = 0;
        String maxKey = null;
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = (Map.Entry) it.next();
            Integer value = entry.getValue();
            if (value > maxValue) {
                maxValue = value;
                maxKey = entry.getKey();
            }
        }
        System.out.println(maxKey + ":" + maxValue);

    }

    @Test
    public void sum() {
        JavaRDD<String> lines = sc.textFile("d:\\hadoop\\num.txt");
        JavaRDD<Integer> numsInt = lines.flatMap(line -> Arrays.asList(line.split("\\s+"))).map(s -> Integer.parseInt(s)).cache();
        Integer sum = numsInt.reduce((a, b) -> a + b);
        System.out.println("sum : "+sum);
        System.out.println(lines.countByValue());
        System.out.println(numsInt.countByValue());
    }

    class AvgCount implements Serializable {
        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }
        public int total;
        public int num;
        public double avg() {
            return total / (double) num;
        }
    }

    @Test
    public void avg() {
        JavaRDD<String> lines = sc.textFile("d:\\hadoop\\num.txt");
        JavaRDD<Integer> numsInt = lines.flatMap(line -> Arrays.asList(line.split("\\s+")))
                .map(s -> Integer.parseInt(s));

        // 默认情况下 persist() 会把数据以序列化的形式缓存在 JVM 的堆空，末尾加上“_2”来把持久化数据存为两份
        numsInt.persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        // fold() 和 reduce() 都要求函数的返回值类型需要和我们所操作的 RDD 中的元素类型相同。
        // aggregate() 函数则把我们从返回值类型必须与所操作的 RDD 类型相同的限制中解放出来。
        AvgCount result = numsInt.aggregate(new AvgCount(0, 0),
                (x, t) -> {x.total += t; x.num += 1; return x;},
                (y, z) -> {y.total += z.total; y.num += z.num; return y;});
        System.out.println(result.avg());
        numsInt.foreach(i -> i=i*i);
        List<Integer> list = numsInt.takeSample(false,2);
        System.out.println(list);
    }



}



