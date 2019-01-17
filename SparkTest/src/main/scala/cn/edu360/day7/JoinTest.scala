package cn.edu360.day7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by zx on 2017/10/14.
  */
object JoinTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhoa,china", "2,laoduan,usa", "3,laoyang,jp"))
    //对数据进行整理

    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })
    val df1 = tpDs.toDF("id", "name", "nation")

    val nations: Dataset[String] = spark.createDataset(List("china,中国", "usa,美国"))
    //对数据进行整理
    val ndataset: Dataset[(String, String)] = nations.map(l => {
      val fields = l.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })

    val df2 = ndataset.toDF("ename","cname")


    df2.count()

    //第一种，创建视图
    //df1.createTempView("v_users")
    //df2.createTempView("v_nations")
    //val r: DataFrame = spark.sql("SELECT name, cname FROM v_users JOIN v_nations ON nation = ename")


    //import org.apache.spark.sql.functions._
    val r = df1.join(df2, $"nation" === $"ename", "left_outer")

    //

   r.show()

    spark.stop()

  }
}
