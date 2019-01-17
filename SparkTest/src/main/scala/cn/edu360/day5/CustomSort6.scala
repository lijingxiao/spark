package cn.edu360.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/10.
  */
object CustomSort6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CustomSort6").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    //将Driver端的数据并行化变成RDD
    val lines: RDD[String] = sc.parallelize(users)

    //切分整理数据
    val tpRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      (name, age, fv)
    })

    //充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
    //Ordering[(Int, Int)]最终比较的规则格式
    //on[(String, Int, Int)]未比较之前的数据格式
    //(t =>(-t._3, t._2))怎样将规则转换成想要比较的格式
    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t =>(-t._3, t._2))
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => tp)

    println(sorted.collect().toBuffer)

    sc.stop()

  }

}
