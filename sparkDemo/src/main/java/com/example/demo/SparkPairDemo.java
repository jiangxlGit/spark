package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author jiangxinlin
 * @version 2018-06-06
 */
public class SparkPairDemo implements Serializable{

    private transient JavaSparkContext sc;
    private transient JavaPairRDD<String, Integer> rdd_1 = null;
    private transient JavaPairRDD<String, String> rdd_2 = null;

    @Before
    public void before() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("spark pairRDD demo");
        sc = new JavaSparkContext(conf);

        // 创建方式1
        rdd_1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("jiang", 5200000),
                new Tuple2<>("jiang", 1314),
                new Tuple2<>("ding", 5201314)));
        System.out.println("rdd_1: " + rdd_1.collect());

        // 创建方式2
        JavaRDD<String> lines = sc.textFile("d:\\spark\\spark\\pair.txt");
        // 把普通的rdd转化成pair rdd
        rdd_2 = lines.mapToPair((s) -> new Tuple2<>(s.split("\\s+")[0], s));
        System.out.println("rdd_2: " + rdd_2.collect());
    }

    private class AvgCount implements Serializable {
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

    // 键值对RDD转化操作
    @Test
    public void pairRDD() {

        // 合并具有相同键的值
        JavaPairRDD<String, Integer> pairRDD_reduceByKey = rdd_1.reduceByKey((s, t) -> s + t);
        System.out.println("pairRDD_reduceByKey: " + pairRDD_reduceByKey.collect());

        // 对具有相同键进行分组
        JavaPairRDD<String, Iterable<String>> pairRDD_groupByKey = rdd_2.groupByKey();
        System.out.println("pairRDD_groupByKey: " + pairRDD_groupByKey.collect());

        // 使用不同的返回类型合并具有相同键的值 combineByKey(createCombiner, mergeValue, mergeCombiners)
        JavaPairRDD<String, AvgCount> avgRDD = rdd_1.combineByKey(v -> new AvgCount(v, 1),
                (c, v) -> {
                    c.total += v;
                    c.num += 1;
                    return c;
                },
                (c1, c2) -> {
                    c1.total += c2.total;
                    c1.num += c2.num;
                    return c1;
                });

        List<Tuple2<String,AvgCount>> list = avgRDD.collect();
        for (Tuple2<String,AvgCount> t : list) {
            String key = t._1();
            AvgCount avgCount = t._2();
            double avg = avgCount.avg();
            System.out.println(key + ":" + avg);
        }
    }


}
