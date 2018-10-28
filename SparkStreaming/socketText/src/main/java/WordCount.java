import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by lijingxiao on 2018/10/18.
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("word.txt");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordOne = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1));
        JavaPairRDD<String, Integer> res = wordOne.reduceByKey((m, n) -> m + n);
        System.out.println(res.collect());
    }
}
