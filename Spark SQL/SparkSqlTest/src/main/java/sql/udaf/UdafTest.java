package sql.udaf;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Created by lijingxiao on 2018/10/15.
 */
public class UdafTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("udaftest").master("local[4]").getOrCreate();
        Dataset<Long> range = spark.range(1, 11);
        try {
            range.createTempView("v_range");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        spark.udf().register("gm", new GeoMean());
        Dataset<Row> result = spark.sql("SELECT gm(id) result FROM v_range");
        result.show();
        spark.stop();
    }
}
