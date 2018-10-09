package study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by lijingxiao on 2018/9/26.
 */
public class CustomSort1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("customSort").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
        List<String> users = Arrays.asList("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99");

        JavaRDD<String> lines = jsc.parallelize(users);
        JavaPairRDD<User, String> userRDD = lines.mapToPair(line -> {
            String[] fileds = line.split(" ");
            String name = fileds[0];
            Integer age = Integer.valueOf(fileds[1]);
            Integer fv = Integer.valueOf(fileds[2]);
            User user = new User(name, age, fv);
            return new Tuple2<>(user, "");
        });

        JavaPairRDD<User, String> sorted = userRDD.sortByKey();
//        JavaRDD<String> retRDD = sorted.map(pair -> {
//            return pair._2();
//        });
//
//        retRDD.foreach( tp -> {
//            System.out.println(tp);
//        });

        System.out.println(sorted.collect());
        jsc.stop();
    }
}
