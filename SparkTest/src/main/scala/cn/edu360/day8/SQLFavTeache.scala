package cn.edu360.day8

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by zx on 2017/9/19.
  */
object SQLFavTeacher {

  def main(args: Array[String]): Unit = {

    val topN = args(1).toInt

    val spark = SparkSession.builder().appName("RowNumberDemo")
      .master("local[4]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile(args(0))

    import spark.implicits._

    val df: DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost
      //学科的index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)
      (subject, teacher)
    }).toDF("subject", "teacher")

    df.createTempView("v_sub_teacher")

    //该学科下的老师的访问次数
    val temp1: DataFrame = spark.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher GROUP BY subject, teacher")

    //求每个学科下最受欢迎的老师的topn
    temp1.createTempView("v_temp_sub_teacher_counts")

    //val temp2 = spark.sql(s"SELECT * FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk, rank() over(order by counts desc) g_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")

    //val temp2 = spark.sql(s"SELECT *, row_number() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")

    val temp2 = spark.sql(s"SELECT *, dense_rank() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")


    temp2.show()



    spark.stop()
  }
}
