package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by lijingxiao on 2018/10/11.
 */
public class SQLTest {
    public static void main(String[] args) {
        //spark2.x SQL的编程API(SparkSession)
        //是spark2.x SQL执行的入口
        SparkSession sparkSession = SparkSession.builder().appName("sqlTest").master("local[2]").getOrCreate();

        //创建特殊的RDD（DataFrame），就是有schema信息的RDD
        //先有一个普通的RDD，然后在关联上schema，进而转成DataFrame
        JavaRDD<String> lines = sparkSession.read().textFile("person.txt").javaRDD();
        JavaRDD<Row> rowRDD = lines.map(line -> {
            String[] fileds = line.split(",");
            Long id = Long.valueOf(fileds[0]);
            String name = fileds[1];
            Integer age = Integer.valueOf(fileds[2]);
            Double fv = Double.valueOf(fileds[3]);
            return RowFactory.create(id, name, age, fv);
        });

        //结果类型，其实就是表头，用于描述DataFrame
        ArrayList<StructField> fileds = new ArrayList<>();
        fileds.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        fileds.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fileds.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        fileds.add(DataTypes.createStructField("fv", DataTypes.DoubleType, true));

        StructType schema = DataTypes.createStructType(fileds);

        Dataset<Row> bdf = sparkSession.createDataFrame(rowRDD, schema);

        //不使用SQL的方式，就不用注册临时表了
        Dataset<Row> select = bdf.select("name", "age", "fv");
        Dataset<Row> ordered = select.orderBy(bdf.col("fv").desc(), bdf.col("age").asc());

        ordered.show();
        sparkSession.stop();
    }
}
