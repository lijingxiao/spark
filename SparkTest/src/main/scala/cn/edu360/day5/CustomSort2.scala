package cn.edu360.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/10.
  */
object CustomSort2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CustomSort2").setMaster("local[*]")

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

    //排序(传入了一个排序规则，不会改变数据的格式，只会改变顺序)
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => new Boy(tp._2, tp._3))

    println(sorted.collect().toBuffer)

    sc.stop()

  }

}


class Boy(val age: Int, val fv: Int) extends Ordered[Boy] with Serializable {

  override def compare(that: Boy): Int = {
    if(this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }
}
