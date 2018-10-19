package sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by lijingxiao on 2018/10/12.
 */
public class JoinTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("joinTest").master("local[2]").getOrCreate();
        Dataset<String> lines = spark.createDataset(Arrays.asList("1,laozhao,china", "2,laoduan,usa", "3,laoyang,jp"), Encoders.STRING());
        JavaRDD<String> linesRDD = lines.toJavaRDD();
        JavaRDD<Row> rowRDD = linesRDD.map(line -> {
            String[] fileds = line.split(",");
            Long id = Long.valueOf(fileds[0]);
            String name = fileds[1];
            String nationCode = fileds[2];
            return RowFactory.create(id, name, nationCode);
        });

        ArrayList<StructField> fileds = new ArrayList<>();
        fileds.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        fileds.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fileds.add(DataTypes.createStructField("nation", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fileds);
        Dataset<Row> df1 = spark.createDataFrame(rowRDD, schema);
//        df1.show();

        JavaRDD<String> nationRDD = spark.createDataset(Arrays.asList("china,中国", "usa,美国"), Encoders.STRING()).toJavaRDD();
        JavaRDD<Row> nRDD = nationRDD.map(line -> {
            String[] nfileds = line.split(",");
            String ename = nfileds[0];
            String cname = nfileds[1];
            return RowFactory.create(ename, cname);
        });
        ArrayList<StructField> nafileds = new ArrayList<>();
        nafileds.add(DataTypes.createStructField("ename", DataTypes.StringType, true));
        nafileds.add(DataTypes.createStructField("cname", DataTypes.StringType, true));
        StructType naschema = DataTypes.createStructType(nafileds);
        Dataset<Row> df2 = spark.createDataFrame(nRDD, naschema);

        try {
            df1.createTempView("v_users");
            df2.createTempView("v_nations");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        df2.cache().count();

//        Dataset<Row> df = spark.sql("SELECT name, cname FROM v_users JOIN v_nations ON nation = ename");
        Dataset<Row> df = df1.join(df2, df1.col("nation").equalTo(df2.col("ename")));

        df.explain();
        df.show();
        spark.stop();
    }
}
