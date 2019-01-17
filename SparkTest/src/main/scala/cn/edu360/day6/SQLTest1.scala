package cn.edu360.day6

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by zx on 2017/10/13.
  */
object SQLTest1 {

  def main(args: Array[String]): Unit = {

    //spark2.x SQL的编程API(SparkSession)
    //是spark2.x SQL执行的入口
    val session = SparkSession.builder()
      .appName("SQLTest1")
      .master("local[*]")
      .getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://node-4:9000/person")

    //将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    //结果类型，其实就是表头，用于描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    //创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._

    val df2: Dataset[Row] = df.where($"fv" > 98).orderBy($"fv" desc, $"age" asc)

    df2.show()

    session.stop()


  }
}
