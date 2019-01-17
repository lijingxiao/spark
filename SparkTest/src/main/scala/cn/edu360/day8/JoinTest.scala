package cn.edu360.day8

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zx on 2017/10/16.
  */
object JoinTest {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //import org.apache.spark.sql.functions._

    //spark.sql.autoBroadcastJoinThreshold=-1
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    //spark.conf.set("spark.sql.join.preferSortMergeJoin", true)

    //println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    val df1 = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "join")
    ).toDF("id", "token")

    val df2 = Seq(
      (0, "P"),
      (1, "W"),
      (2, "S")
    ).toDF("aid", "atoken")


    df2.repartition()

    //df1.cache().count()

    val result: DataFrame = df1.join(df2, $"id" === $"aid")

    //查看执行计划
    result.explain()

    result.show()

    spark.stop()


  }

}
