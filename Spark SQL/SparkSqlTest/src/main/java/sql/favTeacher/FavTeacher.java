package sql.favTeacher;

import com.sun.org.apache.xml.internal.dtm.DTMAxisIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.net.URL;
import java.util.ArrayList;

/**
 * Created by lijingxiao on 2018/10/16.
 */
public class FavTeacher {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("favteacher").master("local[2]").getOrCreate();
        JavaRDD<String> lines = spark.read().textFile("teacher.log").toJavaRDD();
        JavaRDD<Row> tRDD = lines.map(line -> {
            int index = line.lastIndexOf("/");
            String tescher = line.substring(index + 1);
            String host = new URL(line).getHost();
            int sindex = host.indexOf(".");
            String subjects = host.substring(0, sindex);
            return RowFactory.create(tescher, subjects);
        });

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("teacher", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("subject", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> df1 = spark.sqlContext().createDataFrame(tRDD, structType);

        try {
            df1.createTempView("v_teacher");
            Dataset<Row> df2 = spark.sql("select teacher,subject,count(*) counts from v_teacher group by teacher,subject");
            df2.createTempView("v_topn");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        Dataset<Row> df3 = spark.sql("select teacher,subject,counts,row_number() over (partition by subject order by counts desc) su_rk" +
                ",rank() over(order by counts desc) rk from v_topn");

        Dataset<Row> df4 = spark.sql("SELECT *, dense_rank() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, " +
                "row_number() over(partition by subject order by counts desc) sub_rk FROM v_topn) temp2 WHERE sub_rk <= 3 order by subject,g_rk");
        df4.show();

        spark.stop();

    }
}
