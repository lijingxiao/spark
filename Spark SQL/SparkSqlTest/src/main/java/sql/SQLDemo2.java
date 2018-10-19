package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * Created by lijingxiao on 2018/10/10.
 */
public class SQLDemo2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SQLDemo1").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //sparkContext不能创建特殊的RDD（DataFrame）
        //将SparkContext包装进而增强
        SQLContext sqlContext = new SQLContext(jsc);

        //创建特殊的RDD（DataFrame），就是有schema信息的RDD
        //先有一个普通的RDD，然后在关联上schema，进而转成DataFrame
        JavaRDD<String> lines = jsc.textFile("person.txt");
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

        Dataset<Row> bdf = sqlContext.createDataFrame(rowRDD, schema);

        //变成DF后就可以使用两种API进行编程了
        //把DataFrame先注册临时表
        bdf.registerTempTable("t_Boy");

        //书写SQL（SQL方法应其实是Transformation， 一个DataFrame转换成另一个DataFrame）
        Dataset<Row> result = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv desc, age asc");

        result.show();
        jsc.stop();
    }
}
