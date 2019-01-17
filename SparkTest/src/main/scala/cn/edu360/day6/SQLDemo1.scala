package cn.edu360.day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/13.
  */
object SQLDemo1 {

  def main(args: Array[String]): Unit = {

    //提交的这个程序可以连接到Spark集群中
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[2]")

    //创建SparkSQL的连接（程序执行的入口）
    val sc = new SparkContext(conf)
    //sparkContext不能创建特殊的RDD（DataFrame）
    //将SparkContext包装进而增强
    val sqlContext = new SQLContext(sc)
    //创建特殊的RDD（DataFrame），就是有schema信息的RDD

    //先有一个普通的RDD，然后在关联上schema，进而转成DataFrame

    val lines = sc.textFile("hdfs://node-4:9000/person")
    //将数据进行整理
    val boyRDD: RDD[Boy] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Boy(id, name, age, fv)
    })
    //该RDD装的是Boy类型的数据，有了shcma信息，但是还是一个RDD
    //将RDD转换成DataFrame
    //导入隐式转换
    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF

    //变成DF后就可以使用两种API进行编程了
    //把DataFrame先注册临时表
    bdf.registerTempTable("t_boy")

    //书写SQL（SQL方法应其实是Transformation）
    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv desc, age asc")

    //查看结果（触发Action）
    result.show()

    sc.stop()


    
  }
}

case class Boy(id: Long, name: String, age: Int, fv: Double)
