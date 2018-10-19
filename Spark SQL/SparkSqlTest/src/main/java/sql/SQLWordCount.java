package sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by lijingxiao on 2018/10/11.
 */
public class SQLWordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sqlWordCount").master("local[2]").getOrCreate();

        //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
        //dataset只有一列，默认这列叫value
        Dataset<String> lines = spark.read().textFile("words.txt");
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }, Encoders.STRING());

        try {
            words.createTempView("v_wc");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        Dataset<Row> result = spark.sql("select value,count(*) counts from v_wc group by value order by counts desc");

        result.show();
        spark.stop();

    }
}
