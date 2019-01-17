package cn.edu360.day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zx on 2017/10/9.
  */
object IpLoaction1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("IpLoaction1").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //将Drive端的数据广播到Executor中

    //调用sc上的广播方法
    //广播变量的引用（还在Driver端）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile(args(1))

    //这个函数是在哪一端定义的？（Driver）
    val func = (line: String) => {
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //查找
      var province = "未知"
      val index = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    }

    //整理数据
    val proviceAndOne: RDD[(String, Int)] = accessLines.map(func)

    //聚合
    //val sum = (x: Int, y: Int) => x + y
    val reduced: RDD[(String, Int)] = proviceAndOne.reduceByKey(_+_)

    //将结果打印
    val r = reduced.collect()

    println(r.toBuffer)

    sc.stop()



  }
}
