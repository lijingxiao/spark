package study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

/**
 * Created by lijingxiao on 2018/9/26.
 */
public class CustomSort2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CustomSort2").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
        List<String> users = Arrays.asList("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99");

        JavaRDD<String> lines = jsc.parallelize(users);
//        JavaPairRDD<Tuple3<String, Integer, Integer>, String> tpRDD = lines.mapToPair(line -> {
//            String[] fileds = line.split(" ");
//            String name = fileds[0];
//            Integer age = Integer.valueOf(fileds[1]);
//            Integer fv = Integer.valueOf(fileds[2]);
//            Tuple3<String, Integer, Integer> tp = new Tuple3<>(name, age, fv);
//            return new Tuple2<>(tp, "");
//        });

        JavaRDD<Tuple3> tpRDD = lines.map(line -> {
            String[] fileds = line.split(" ");
            String name = fileds[0];
            Integer age = Integer.valueOf(fileds[1]);
            Integer fv = Integer.valueOf(fileds[2]);
            return new Tuple3(name, age, fv);
        });

        //传入一个可排序的类型
        //java.lang.ClassCastException: scala.Tuple3 cannot be cast to java.lang.Comparable???
//        tpRDD.sortBy( tp -> (tp._3(), tp._2()));
        JavaRDD<Tuple3> sorted = tpRDD.sortBy(tp -> tp, false, 1);
        System.out.println(sorted.collect());
        jsc.stop();
    }
}
