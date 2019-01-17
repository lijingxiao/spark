package cn.edu360.ser

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/6/25.
  */
object SerTest {

  def main(args: Array[String]): Unit = {

    //在Driver端被实例化
    //val rules = new Rules

    //初始化object（在Driver端）
    //var rules = Rules
    //println("@@@@@@@@@@@@" + rules.toString + "@@@@@@@@@@@@")

    val conf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val r = lines.map(word => {
      //在map的函数中，创建一个rules实例(太浪费资源)
      //val rules = new Rules
      //函数的执行是在Executor执行的（Task中执行的）
      val hostname = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      //rules的实际是在Executor中使用的
      (hostname, threadName, Rules.rulesMap.getOrElse(word, 0), Rules.toString)
    })

    r.saveAsTextFile(args(1))

    sc.stop()

  }

}
