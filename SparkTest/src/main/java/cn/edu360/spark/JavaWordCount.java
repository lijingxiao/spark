package cn.edu360.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zx on 2017/10/5.
 */
public class JavaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //将单词和一组合在一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //调换顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                //return new Tuple2<>(tp._2, tp._1);
                return tp.swap();
            }
        });

        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });

        //将数据保存到hdfs
        result.saveAsTextFile(args[1]);

        //释放资源
        jsc.stop();



    }
}
