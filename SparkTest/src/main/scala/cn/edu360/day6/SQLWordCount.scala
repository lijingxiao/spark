package cn.edu360.day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by zx on 2017/10/13.
  */
object SQLWordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()

    //(指定以后从哪里)读数据，是lazy

    //Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //dataset只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("hdfs://node-4:9000/words")

    //整理数据(切分压平)
    //导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //注册视图
    words.createTempView("v_wc")

    //执行SQL（Transformation，lazy）
    val result: DataFrame = spark.sql("SELECT value word, COUNT(*) counts FROM v_wc GROUP BY word ORDER BY counts DESC")

    //执行Action
    result.show()

    spark.stop()

  }
}
