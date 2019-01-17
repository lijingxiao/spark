package cn.edu360.day7

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by zx on 2017/5/13.
  */
object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "logs",
        "user" -> "root",
        "password" -> "123568")
    ).load()

    //logs.printSchema()


    //logs.show()

//    val filtered: Dataset[Row] = logs.filter(r => {
//      r.getAs[Int]("age") <= 13
//    })
//    filtered.show()

    //lambda表达式
    val r = logs.filter($"age" <= 13)

    //val r = logs.where($"age" <= 13)

    val reslut: DataFrame = r.select($"id", $"name", $"age" * 10 as "age")

    //val props = new Properties()
    //props.put("user","root")
    //props.put("password","123568")
    //reslut.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "logs1", props)

    //DataFrame保存成text时出错(只能保存一列)
    //reslut.write.text("/Users/zx/Desktop/text")

    //reslut.write.json("/Users/zx/Desktop/json")

    //reslut.write.csv("/Users/zx/Desktop/csv")

    //reslut.write.parquet("hdfs://node-4:9000/parquet")


    //reslut.show()

    spark.close()


  }
}
